/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.Pair;
import org.geoserver.platform.GeoServerExtensions;
import org.geotools.util.logging.Logging;

/**
 * The global configuration lock. At the moment it is called by coarse grained request level
 * callbacks to lock both the GUI and the REST configuration so that concurrent access does not end
 * up causing issues (like corrupt configuration and the like).
 *
 * <p>The locking code can be disabled by calling {@link
 * GeoServerConfigurationLock#setEnabled(boolean)} or by setting the system variable
 * {code}-DGeoServerConfigurationLock.enabled=false{code}
 *
 * @author Andrea Aime - GeoSolution
 *     <p>In case lock is held for longer than <code>DEAD_LOCK_TRIGGER_TS</code>, lock will be
 *     replaced with the new one as the locks held longer than configured time are considered to be
 *     `lost`.
 *     <p>All threads that were attempting to acquire a lock during this time will perform
 *     reattempts using the new lock.
 */
public class GeoServerConfigurationLock {

    /** DEFAULT_TRY_LOCK_TIMEOUT_MS */
    public static long DEFAULT_TRY_LOCK_TIMEOUT_MS =
            (GeoServerExtensions.getProperty("CONFIGURATION_TRYLOCK_TIMEOUT") != null
                    ? Long.valueOf(GeoServerExtensions.getProperty("CONFIGURATION_TRYLOCK_TIMEOUT"))
                    : 30000);

    private static final Level LEVEL = Level.FINE;

    private static final Logger LOGGER = Logging.getLogger(GeoServerConfigurationLock.class);

    private static volatile ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    private static final ThreadLocal<LockType> currentLock = new ThreadLocal<>();

    public enum LockType {
        READ,
        WRITE
    };

    private boolean enabled;

    // Deadlock resolution variables
    private static final Map<Thread, Pair<LockType, Long>> lockTimes = new ConcurrentHashMap<>();
    // Any lock held for longer than a minute is considered to be lost (deadlocked), owning thread
    // will be
    // interrupted and new lock will be created for further use
    private static final long DEADLOCK_TRIGGER_TS = 60000;
    private static final AtomicLong lockCreateTs = new AtomicLong(System.currentTimeMillis());
    private static final Object LOCK_OBJECT = new Object();

    public GeoServerConfigurationLock() {
        String pvalue = System.getProperty("GeoServerConfigurationLock.enabled");
        if (pvalue != null) {
            enabled = Boolean.parseBoolean(pvalue);
        } else {
            enabled = true;
        }

        LOGGER.info("GeoServer configuration lock is " + (enabled ? "enabled" : "disabled"));
    }

    /**
     * Opens a lock in the specified mode. To avoid deadlocks make sure the corresponding unlock
     * method is called as well before the code exits
     */
    public void lock(LockType type) {
        if (!enabled) {
            return;
        }

        boolean locked = tryLock(type);

        if (!locked) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(
                        Level.SEVERE,
                        "Failed to acquire lock="
                                + type
                                + ", thread="
                                + Thread.currentThread().getName());
            }
            throw new RuntimeException("Failed to acquire lock=" + type);
        }

        if (LOGGER.isLoggable(LEVEL)) {
            LOGGER.log(
                    LEVEL,
                    "Thread " + Thread.currentThread().getId() + " got the lock in mode " + type);
        }
    }

    /**
     * Tries to open a lock in the specified mode. Acquires the lock if it is available and returns
     * immediately with the value true. If the lock is not available then this method will return
     * immediately with the value false.
     *
     * <p>A typical usage idiom for this method would be:
     *
     * <pre>
     *  Lock lock = ...;
     *  if (lock.tryLock()) {
     *    try {
     *      // manipulate protected state
     *    } finally {
     *      lock.unlock();
     *    }
     *  } else {
     *    // perform alternative actions
     *  }}
     * </pre>
     *
     * This usage ensures that the lock is unlocked if it was acquired, and doesn't try to unlock if
     * the lock was not acquired.
     *
     * @return true if the lock was acquired and false otherwise
     */
    public boolean tryLock(LockType type) {
        if (!enabled) {
            return true;
        }

        // Capture the creation timestamp as it will be used to detect deadlock scenario
        final long startTs = System.currentTimeMillis();

        boolean success = false;
        boolean retry = false;
        try {
            success = getLock(type).tryLock(DEFAULT_TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // In case of failure retry locking by checking for deadlocks
            if (!success) {
                success = tryLockReplaceAndAcquire(type);
                retry = true;
            }
        } catch (InterruptedException e) {
            LOGGER.log(
                    Level.WARNING,
                    "Thread "
                            + Thread.currentThread().getId()
                            + " thrown an InterruptedException on GeoServerConfigurationLock TryLock.",
                    e);
        } finally {
            // Set lock variables
            if (success) {
                currentLock.set(type);
                lockTimes.put(Thread.currentThread(), Pair.of(type, System.currentTimeMillis()));
            }
        }

        if (LOGGER.isLoggable(LEVEL)) {
            if (success) {
                LOGGER.log(
                        LEVEL,
                        "Thread "
                                + Thread.currentThread().getId()
                                + " got the lock in mode "
                                + type);
            } else {
                LOGGER.log(
                        LEVEL,
                        "Thread "
                                + Thread.currentThread().getId()
                                + " could not get the lock in mode "
                                + type);
            }
        }

        // In case of a retry success, we do not care about lock timestamp as the new timestamp was
        // set in the retry
        // and checking it would always yield false positive which would incur a lock retry
        if (retry && success) {
            return success;
        }

        // Retry the lock in case lock was changed as this lock is no longer valid
        if (lockCreateTs.get() > startTs) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(
                        Level.SEVERE,
                        "Reacquiring the lock due to lock replacement, lock="
                                + type
                                + ", thread="
                                + Thread.currentThread().getName());
            }
            if (success) {
                // Unset lock variables
                currentLock.set(null);
                lockTimes.remove(Thread.currentThread());
            }
            return tryLock(type);
        }

        return success;
    }

    private boolean isPotentialDeadlock() {
        long lockAge = System.currentTimeMillis() - lockCreateTs.get();
        // In case lock is not old enough to deadlock, return (this can happen only in case other
        // thread already
        // replaced the deadlocked lock)
        return lockAge > DEADLOCK_TRIGGER_TS;
    }

    private boolean tryLockReplaceAndAcquire(LockType lockType) {
        // No point to check
        if (!isPotentialDeadlock()) {
            return false;
        }

        Map.Entry<Thread, Pair<LockType, Long>> deadlockEntry = null;
        for (Map.Entry<Thread, Pair<LockType, Long>> acquiredLocks : lockTimes.entrySet()) {
            long lockTs = acquiredLocks.getValue().getValue();
            long elapsedTime = System.currentTimeMillis() - lockTs;

            if (elapsedTime > DEADLOCK_TRIGGER_TS) {
                deadlockEntry = acquiredLocks;
                break;
            }
        }

        // Deadlock scenario
        if (deadlockEntry != null) {
            synchronized (LOCK_OBJECT) {
                // In case lock is not old enough to deadlock, return (this can happen only in case
                // other thread already
                // replaced the deadlocked lock)
                if (!isPotentialDeadlock()) {
                    return false;
                }

                // We no longer care for current locks (somewhat dangerous)
                lockTimes.clear();
                // Replace the lock with new one
                ReentrantReadWriteLock newLock = new ReentrantReadWriteLock(true);

                // Acquire the needed lock
                if (lockType == LockType.READ) {
                    newLock.readLock().lock();
                } else {
                    newLock.writeLock().lock();
                }

                // Make the lock available to other threads
                readWriteLock = newLock;

                // Set the time when lock was replaced
                lockCreateTs.set(System.currentTimeMillis());

                // Interrupt the lock owning thread
                if (deadlockEntry.getKey().isAlive()) {
                    try {
                        deadlockEntry.getKey().interrupt();
                    } catch (Exception e) {
                        LOGGER.log(
                                Level.WARNING,
                                "Failed to interrupt stuck thread="
                                        + deadlockEntry.getKey().getName(),
                                e);
                    }
                }

                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(
                            Level.SEVERE,
                            "Caught potential deadlock, lock_owner_thread="
                                    + deadlockEntry.getKey().getName()
                                    + ", lock_owner_thread_state="
                                    + deadlockEntry.getKey().getState()
                                    + ", lock_type="
                                    + deadlockEntry.getValue().getKey()
                                    + ", thread="
                                    + Thread.currentThread().getName());
                }

                return true;
            }
        }
        return false;
    }

    /**
     * Tries to upgrade the current read lock to a write lock. If the current lock is not a read
     * one, it will throw an {@link IllegalStateException}
     */
    public void tryUpgradeLock() {
        LockType lock = currentLock.get();
        if (lock == null) {
            throw new IllegalStateException("No lock currently held");
        } else if (lock == LockType.WRITE) {
            throw new IllegalStateException("Already owning a write lock");
        } else {
            // core java does not have a notion of lock upgrade, one has to release the
            // read lock and get a write one
            unlock();
            if (tryLock(LockType.WRITE)) {
                currentLock.set(LockType.WRITE);
            } else {
                currentLock.set(null);
                throw new RuntimeException(
                        "Failed to upgrade lock from read to write "
                                + "state, please re-try the configuration operation");
            }
        }
    }

    /**
     * Unlocks a previously acquired lock. The lock type must match the previous {@link
     * #lock(LockType)} call
     */
    public void unlock() {
        if (!enabled) {
            return;
        }

        final LockType type = getCurrentLock();
        if (type == null) {
            return;
        }
        try {
            Lock lock = getLock(type);

            /*
                        if (LOGGER.isLoggable(LEVEL)) {
                LOGGER.log(
                        LEVEL,
                        "Thread "
                                + Thread.currentThread().getId()
                                + " releasing the lock in mode "
                                + type);
            }
             */

            lock.unlock();
            lockTimes.remove(Thread.currentThread());
        } finally {
            currentLock.set(null);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** */
    private Lock getLock(LockType type) {
        Lock lock;
        if (type == LockType.WRITE) {
            lock = readWriteLock.writeLock();
        } else {
            lock = readWriteLock.readLock();
        }
        /*
                if (LOGGER.isLoggable(LEVEL)) {
            LOGGER.log(
                    LEVEL, "Thread " + Thread.currentThread().getId() + " locking in mode " + type);
        }
         */
        return lock;
    }

    /** Returns the lock type owned by the current thread (could be {@code null} for no lock) */
    public LockType getCurrentLock() {
        return currentLock.get();
    }

    public static void main(String[] args) throws Exception {
        final AtomicBoolean writing = new AtomicBoolean(false);

        GeoServerConfigurationLock lock = new GeoServerConfigurationLock();

        AtomicInteger writeReqs = new AtomicInteger(0);
        AtomicInteger readReqs = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            final int tid = i;
            new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    Thread.currentThread().setName("R: " + tid);
                                    while (true) {
                                        try {
                                            lock.lock(LockType.WRITE);
                                            if (writing.get()) {
                                                System.out.println(
                                                        "FAIL FROM WRITER, THERE IS A THREAD OWNING WRITE LOCK");
                                            }
                                            writing.set(true);
                                            try {
                                                Thread.sleep(10);
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            writing.set(false);

                                            boolean deadlock = false;
                                            if (tid == 0
                                                    && ThreadLocalRandom.current().nextInt(100)
                                                            > 98) {
                                                try {
                                                    deadlock = true;
                                                    System.out.println("Simulating deadlock...");
                                                    Thread.sleep(70000);
                                                } catch (Exception e) {
                                                    // e.printStackTrace();
                                                }
                                            }

                                            if (!deadlock) {
                                                lock.unlock();
                                                writeReqs.incrementAndGet();
                                            }

                                        } catch (Exception e) {
                                            // e.printStackTrace();
                                        }
                                    }
                                }
                            })
                    .start();
        }

        for (int i = 0; i < 100; i++) {
            new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    while (true) {
                                        try {
                                            lock.lock(LockType.READ);
                                            if (writing.get()) {
                                                System.out.println(
                                                        "FAIL FROM READER, THERE IS A THREAD OWNING WRITE LOCK");
                                            }
                                            try {
                                                Thread.sleep(1);
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            lock.unlock();
                                            readReqs.incrementAndGet();
                                        } catch (Exception e) {
                                        }
                                    }
                                }
                            })
                    .start();
        }

        int r = readReqs.get();
        int w = writeReqs.get();

        while (true) {
            Thread.sleep(5000);
            int newr = readReqs.get();
            int neww = writeReqs.get();

            int rd = newr - r;
            int wd = neww - w;

            System.out.println("R: " + rd);
            System.out.println("W: " + wd);

            r = newr;
            w = neww;
        }
    }
}

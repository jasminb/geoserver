/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * (c) 2014 OpenPlans
 * (c) 2008-2010 GeoSolutions
 *
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 *
 * Original from GeoWebCache 1.5.1 under a LGPL license
 */
package org.geoserver.platform.resource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.digest.DigestUtils;
import org.geotools.util.logging.Logging;

/**
 * An in memory lock provider based on a striped lock
 *
 * @author Andrea Aime - GeoSolutions
 */
public class MemoryLockProvider implements LockProvider {

    static final Logger LOGGER = Logging.getLogger(MemoryLockProvider.class.getName());

    java.util.concurrent.locks.Lock[] locks;

    public MemoryLockProvider() {
        this(1024);
    }

    public MemoryLockProvider(int concurrency) {
        locks = new java.util.concurrent.locks.Lock[concurrency];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @Override
    public Resource.Lock acquire(String lockKey) {
        return acquire(lockKey, 0l);
    }

    public Resource.Lock acquire(String lockKey, long timeoutMs) {
        final int idx = getIndex(lockKey);
        if (LOGGER.isLoggable(Level.FINE))
            LOGGER.fine("Mapped lock key " + lockKey + " to index " + idx + ". Acquiring lock.");

        boolean acquire = false;

        if (timeoutMs > 0) {
            try {
                acquire = locks[idx].tryLock(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "Waiting for lock interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            locks[idx].lock();
            acquire = true;
        }

        if (!acquire) {
            throw new RuntimeException("Unable to acquire lock!");
        }

        if (LOGGER.isLoggable(Level.FINE))
            LOGGER.fine("Mapped lock key " + lockKey + " to index " + idx + ". Lock acquired");
        return new Resource.Lock() {

            boolean released = false;

            @Override
            public void release() {
                if (!released) {
                    released = true;
                    locks[idx].unlock();
                    if (LOGGER.isLoggable(Level.FINE))
                        LOGGER.fine("Released lock key " + lockKey + " mapped to index " + idx);
                }
            }

            @Override
            public String toString() {
                return "MemoryLock " + idx;
            }
        };
    }

    private int getIndex(String lockKey) {
        // Simply hashing the lock key generated a significant number of collisions,
        // doing the SHA1 digest of it provides a much better distribution
        int idx = Math.abs(DigestUtils.sha1Hex(lockKey).hashCode() % locks.length);
        return idx;
    }

    @Override
    public String toString() {
        return "MemoryLockProvider";
    }
}

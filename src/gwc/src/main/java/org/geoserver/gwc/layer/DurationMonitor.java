package org.geoserver.gwc.layer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.geotools.util.logging.Logging;

/**
 * Simple timer-based monitor
 *
 * @author jbegic
 */
public class DurationMonitor {
    private static final Logger LOGGER = Logging.getLogger(DurationMonitor.class);
    private final String name;
    private static final Timer timer = new Timer(true);
    private final long start;
    private final long maxDuration;
    private final String additionalInfo;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public DurationMonitor(String name, long maxDuration) {
        this(name, maxDuration, Collections.emptyMap());
    }

    public DurationMonitor(String name, long maxDuration, Map<String, String> meta) {
        this.name = name;
        this.additionalInfo = stringify(meta);
        this.start = System.currentTimeMillis();
        this.maxDuration = maxDuration;

        TimerTask timerTask =
                new TimerTask() {
                    @Override
                    public void run() {
                        if (finished.get()) {
                            cancel();
                            return;
                        }

                        long duration = System.currentTimeMillis() - start;
                        LOGGER.info(
                                "Detected slow-running action, name="
                                        + name
                                        + ", duration="
                                        + duration
                                        + additionalInfo);
                    }
                };

        this.timer.schedule(timerTask, maxDuration, maxDuration);
    }

    public void stop() {
        finished.set(true);
        long duration = System.currentTimeMillis() - start;

        if (duration > maxDuration) {
            LOGGER.info(
                    "Slow-running action completed, action="
                            + name
                            + ", duration="
                            + duration
                            + additionalInfo);
        }
    }

    private String stringify(Map<String, String> metadata) {
        StringBuilder builder = new StringBuilder();

        Iterator<Map.Entry<String, String>> entryIterator = metadata.entrySet().iterator();

        while (entryIterator.hasNext()) {
            Map.Entry<String, String> pair = entryIterator.next();
            builder.append(pair.getKey()).append("=").append(pair.getValue());
            if (entryIterator.hasNext()) {
                builder.append(", ");
            }
        }

        if (builder.length() == 0) {
            return builder.toString();
        } else {
            return builder.insert(0, ", ").toString();
        }
    }
}

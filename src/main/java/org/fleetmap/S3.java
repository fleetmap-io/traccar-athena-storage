package org.fleetmap;

import org.traccar.model.Position;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class S3 implements AutoCloseable {

    private final Map<PartitionKey, List<Position>> buffers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public S3() {
        int flushIntervalSeconds = Config.getFlushInterval();
        scheduler.scheduleAtFixedRate(this::flushAll, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    public synchronized void write(Position position) {
        PartitionKey key = new PartitionKey(position.getDeviceId() / 10, DATE_FORMAT.format(position.getFixTime()));
        buffers.computeIfAbsent(key, k -> new ArrayList<>()).add(position);
    }

    public void flushAll() {
        Map<PartitionKey, List<Position>> toFlush = new HashMap<>();

        synchronized (this) {
            for (Map.Entry<PartitionKey, List<Position>> entry : buffers.entrySet()) {
                List<Position> buffer = entry.getValue();
                if (!buffer.isEmpty()) {
                    toFlush.put(entry.getKey(), new ArrayList<>(buffer));
                    buffer.clear();
                }
            }
        }

        for (Map.Entry<PartitionKey, List<Position>> entry : toFlush.entrySet()) {
            ParquetWriter.writeToS3(entry.getValue(), entry.getKey());
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            //noinspection ResultOfMethodCallIgnored
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
        flushAll();
    }
}

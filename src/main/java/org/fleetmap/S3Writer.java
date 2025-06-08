package org.fleetmap;

import org.traccar.model.Position;

import java.util.ArrayList;
import java.util.List;

public class S3Writer {
    private final List<Position> buffer = new ArrayList<>();
    private final int flushThreshold;

    public S3Writer(int flushThreshold) {
        this.flushThreshold = flushThreshold;
    }

    public synchronized void write(Position position) {
        buffer.add(position);
        if (buffer.size() >= flushThreshold) {
            flush();
        }
    }

    public synchronized void flush() {
        if (buffer.isEmpty()) return;
        List<Position> batch = new ArrayList<>(buffer);
        buffer.clear();
        ParquetWriterUtil.writeToS3(batch);
    }
}

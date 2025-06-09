package org.fleetmap;

import java.util.Objects;

public class PartitionKey {
    private final long shard;
    private final String date; // Format: yyyy-MM-dd

    public PartitionKey(long shard, String date) {
        this.shard = shard;
        this.date = date;
    }

    public long getShard() {
        return shard;
    }

    public String getDate() {
        return date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionKey)) return false;
        PartitionKey that = (PartitionKey) o;
        return shard == that.shard && date.equals(that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, date);
    }
}

package org.reset.replication.merkle;

import java.util.Objects;

public final class SimpleLeaf {

    private final long hash;
    private final long timestamp;
    private final boolean tombstone;

    public SimpleLeaf(long hash, long timestamp, boolean tombstone) {
        this.hash = hash;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    public SimpleLeaf(long hash, long timestamp) {
        this(hash, timestamp, false);
    }

    public SimpleLeaf(long hash) {
        this(hash, System.currentTimeMillis());
    }

    public long getHash() {
        return hash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isTombstone() {
        return tombstone;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleLeaf)) return false;
        SimpleLeaf that = (SimpleLeaf) o;

        return hash == that.hash &&
                tombstone == that.tombstone &&
                timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, timestamp, tombstone);
    }

    @Override
    public String toString() {
        return "SimpleLeaf{" +
                "hash=" + hash +
                ", timestamp=" + timestamp +
                ", tombstone=" + tombstone +
                '}';
    }

}

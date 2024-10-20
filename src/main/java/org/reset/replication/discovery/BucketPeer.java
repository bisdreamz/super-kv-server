package org.reset.replication.discovery;

import java.util.List;
import java.util.Objects;

/**
 * Represents a bucket and its assigned replica servers (Peers).
 */
public class BucketPeer {
    private final int bucketId;
    private final List<Peer> replicas;

    public BucketPeer(int bucketId, List<Peer> replicas) {
        this.bucketId = bucketId;
        this.replicas = replicas;
    }

    public int getBucketId() {
        return bucketId;
    }

    public List<Peer> getReplicas() {
        return replicas;
    }

    @Override
    public String toString() {
        return "BucketPeer{" +
                "bucketId=" + bucketId +
                ", replicas=" + replicas +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BucketPeer that = (BucketPeer) o;

        if (bucketId != that.bucketId) return false;
        return Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
        return 31 * bucketId + (replicas != null ? replicas.hashCode() : 0);
    }
}
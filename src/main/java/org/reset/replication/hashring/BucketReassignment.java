package org.reset.replication.hashring;

import org.reset.replication.discovery.Peer;

import java.util.Collections;
import java.util.List;

/**
 * Represents just the changes needed for a bucket reassignment
 * during a peer change event
 */
public class BucketReassignment {

    private final int bucketId;
    private final List<Peer> newReplicas;  // Only the peers that need the data
    private final List<Peer> allReplicas;  // All current peers for this bucket

    public BucketReassignment(int bucketId, List<Peer> newReplicas, List<Peer> allReplicas) {
        this.bucketId = bucketId;

        this.newReplicas = Collections.unmodifiableList(newReplicas);
        this.allReplicas = Collections.unmodifiableList(allReplicas);
    }

    public int getBucketId() {
        return bucketId;
    }

    /**
     * @return List pf Peers from ew replica assignments as a result of
     * some bucket reassignment event. This list is specifically
     * peers which
     */
    public List<Peer> getNewReplicas() {
        return newReplicas;
    }

    public List<Peer> getAllReplicas() {
        return allReplicas;
    }

}
package org.reset.replication.hashring;

import org.reset.replication.discovery.Peer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DataMovementPlan {

    private final int bucketId;  // The fixed bucket ID
    private final Set<Peer> existingHolders;   // Peers that already have this bucket's data
    private final Set<Peer> newRecipients;     // Peers that need this bucket's data

    public DataMovementPlan(int bucketId, Set<Peer> existingHolders, Set<Peer> newRecipients) {
        this.bucketId = bucketId;
        this.existingHolders = Collections.unmodifiableSet(new HashSet<>(existingHolders));
        this.newRecipients = Collections.unmodifiableSet(new HashSet<>(newRecipients));
    }

    public int getBucketId() {
        return bucketId;
    }

    public Set<Peer> getExistingHolders() {
        return existingHolders;
    }

    public Set<Peer> getNewRecipients() {
        return newRecipients;
    }

}
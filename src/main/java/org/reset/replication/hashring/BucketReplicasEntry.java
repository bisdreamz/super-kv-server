package org.reset.replication.hashring;

import org.reset.replication.discovery.Peer;

import java.util.List;

public class BucketReplicasEntry {

    private final List<Peer> peers;
    private final int bucket;

    public BucketReplicasEntry(List<Peer> peers, int bucket) {
        this.peers = peers;
        this.bucket = bucket;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public int getBucket() {
        return bucket;
    }
}

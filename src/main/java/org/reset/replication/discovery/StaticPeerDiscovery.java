package org.reset.replication.discovery;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StaticPeerDiscovery extends AbstractPeerDiscovery {

    private final List<Peer> peers;

    public StaticPeerDiscovery(List<Peer> peers) {
        this.peers = peers;
        // TODO implement and use manual p2p probe for liveness checks
    }

    @Override
    public CompletableFuture<List<Peer>> init() {
        return CompletableFuture.completedFuture(this.peers);
    }

    @Override
    public List<Peer> getPeers() {
        return this.peers;
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

}

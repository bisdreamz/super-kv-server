package org.reset.replication;

import net.openhft.hashing.LongHashFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.replication.discovery.AbstractPeerDiscovery;
import org.reset.replication.discovery.Peer;
import org.reset.replication.discovery.StaticPeerDiscovery;
import org.reset.replication.merkle.SimpleLeaf;
import org.reset.replication.merkle.SimpleMerkle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ReplicationManager {

    private static final Logger log = LogManager.getLogger(ReplicationManager.class);

    // TODO
    // Disruptor queue and bulk transfer of commands to replica nodes
    // Rebalancing
    // Discovery of peer nodes, in particular the peer bucket replica(s) if any
    //
    // Should handle snapshotting of data for replication
    private final SimpleMerkle merkle;
    private final AbstractPeerDiscovery peerDiscovery;
    private final ConsistentHashRing hashRing;
    private final List<Peer> peers;

    public ReplicationManager(LongHashFunction hashFunction, int totalBuckets,
                              int virtualNodesPerServer, int replicationFactor) {
        this.merkle = new SimpleMerkle(totalBuckets, hashFunction);
        this.peerDiscovery = new StaticPeerDiscovery(List.of(new Peer("localhost", true)));
        this.hashRing = new ConsistentHashRing(totalBuckets, virtualNodesPerServer, replicationFactor,
                hashFunction);
        this.peers = new ArrayList<>();

        this.peerDiscovery.onPeerChanged(this::handlePeerChange);
    }

    public CompletableFuture<Void> start() {
        return this.peerDiscovery.init().thenAccept(peers -> {
            if (peers == null || peers.isEmpty())
                throw new IllegalStateException("Peer discovery failed, no peers found");
            else if (peers.size() == 1 && peers.getFirst().isSelf())
                log.info("No peers discovered, but at least I have self!");

            // TODO logic to assign buckets
            this.peers.addAll(peers);

            peers.forEach(this.hashRing::addServer);

            log.info("Discovered {} peers ({}), registered with hashring",
                    peers.size(), String.join(", ", peers.stream().map(p -> p.getHost()).toList()));
        });
    }

    public CompletableFuture<Void> stop() {
        return this.peerDiscovery.shutdown().thenRun(() -> {
            log.info("Peer discovery service closed, unregistered self.");
        }).thenRun(() -> {
            log.debug("Awaiting some time to provide servers to process");

            // do things to rebalance out data here
        });
    }

    /*
        Monitor changes to peers. If one goes down, re-assess buckets and kick off a rebalance
        if needed. If one comes up, assess need to rebalance or replication.
        We rely on the peer discovery service to ensure reliable event notifications here,
        e.g. ensuring confidence in a node being down.
        If a rebalance is already happening and we need to start another,
        we should cancel the current rebalance in place and restart a new job.
     */
    private void handlePeerChange(Peer peer) {
        if (peer.getState() == Peer.State.UP) {
            // if SELF
            //    if NEW NODE send replication data request to peer, if they have any data?
            //    else if NODE RECOVERING send a
        }

        Peer exists = peers.stream().filter(p -> p.getHost().equals(peer.getHost())).findFirst().orElse(null);

        // We should always recognize node events unless its a brand new node coming up
        if (peer.getState() != Peer.State.UP && exists == null) {
            log.warn("Unrecognized peer {} non-UP state change to {}, ignoring!", peer.getHost(), peer.getState());
            return;
        }

        // TODO need to make sure our server peer list is up to date if coming back up before making any decisions
        if (!peer.isSelf()) {
            switch (peer.getState()) {
                case DOWN_MANAGER, DOWN_PROBE:
                    log.info("Peer {} went down for reason {}, removing from peer list",
                            peer.getHost(), peer.getState());
                    this.peers.remove(exists);
                    // TODO update server buckets, kick off rebalance if needed?
                    // watch job to redistribute data if enough servers go down to
                    // reduce bucket counts? e.g. if 2 different bucket replicas go down
                    // we can still operate with only 1 replica per bucket
                    // but if they are eventually down for good then redistribute
                    // so all servers have proper replica counts.
                    // We initiate this if the server that went down was our replica,
                    // then we are responsible for deciding to begin this process
                    break;
                case UP:
                    if (exists != null) {
                        log.warn("Peer {} notice of state change to UP, but already UP. Ignoring!", peer.getHost());
                        break;
                    }
                    this.peers.add(peer);
                    break;
                default:
                    throw new IllegalStateException("Unhandled peer state: " + peer.getState());
            }

            return;
        }

        // This is a notification about the node we are running on (SELF),
        // keep our own entry and handle as needed
        if (peer.getState() == Peer.State.UP) {
            if (exists.getState() != Peer.State.UP) {
                log.info("Self node was down but is back UP, kicking off synchronization checks..");
            } else {
                // Ugh, we are up already?
            }
        } else {
            // Being told we are down lol, hang out until we regain network access or whatever
            // is wrong then can resync and resume service
            log.warn("Received notice that self node is down, hanging out until we can rejoin the cluster..");
        }

        exists.setState(peer.getState());
    }

    public void keyUpdate(byte[] key, byte[] value) {
        SimpleLeaf leaf = this.merkle.set(key, value);
        // TODO add to replication queue, including timestamp from leaf to ensure consistency
    }

    public void keyDelete(byte[] key) {
        SimpleLeaf leaf = this.merkle.delete(key);
        if (leaf == null)
            return; // nothing to do, didnt exist

        // TODO add to replication queue
    }

}

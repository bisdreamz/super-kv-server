package org.reset.replication;

import com.nimbus.net.Node;
import net.openhft.hashing.LongHashFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.replication.discovery.AbstractPeerDiscovery;
import org.reset.replication.discovery.Peer;
import org.reset.replication.hashring.BucketReplicasEntry;
import org.reset.replication.hashring.ConsistentHashRing;
import org.reset.replication.hashring.DataMovementPlan;
import org.reset.replication.merkle.SimpleLeaf;
import org.reset.replication.merkle.SimpleMerkle;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class LocalReplicationCluster extends AbstractReplicationCluster {

    private static final Logger log = LogManager.getLogger(LocalReplicationCluster.class);

    private final AbstractPeerDiscovery peerDiscovery;
    private final SimpleMerkle merkle;
    private final ConsistentHashRing hashRing;
    private final List<Peer> peers;

    public LocalReplicationCluster(AbstractPeerDiscovery peerDiscovery, LongHashFunction hashFunction,
                                   int buckets, int virtNodesPerServer, int replFactor) {
        this.peerDiscovery = peerDiscovery;

        this.merkle = new SimpleMerkle(buckets, hashFunction);
        this.hashRing = new ConsistentHashRing(buckets, virtNodesPerServer, replFactor, hashFunction);
        this.peers = new ArrayList<>();

        this.peerDiscovery.onPeerChanged(this::handlePeerChange);
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
                    Map<Integer, DataMovementPlan> affectedBuckets = this.hashRing.removeServer(exists);

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

    @Override
    public CompletableFuture<Void> start() {
        return this.peerDiscovery.init().thenAccept(peers -> {
            if (peers == null || peers.isEmpty())
                throw new IllegalStateException("Peer discovery failed, no peers found");
            else if (peers.size() == 1 && peers.getFirst().isSelf())
                log.info("No peers discovered, but at least I have self!");

            peers.forEach(this.hashRing::addServer);

            log.info("Discovered {} peers ({}), registered with hashring and merkle tree",
                    peers.size(), String.join(", ", peers.stream().map(p -> p.getHost()).toList()));
        });
    }

    @Override
    public int set(long keyHash, long hashValue) {
        BucketReplicasEntry bucketEntry = this.hashRing.getReplicasForKey(keyHash);
        if (bucketEntry == null)
            throw new IllegalStateException("Bucket replicas for " + keyHash + " not found?!");

        // this doesnt belong to us!
        if (!bucketEntry.getPeers().stream().anyMatch(p -> p.isSelf()))
            return -1;

        int bucket = bucketEntry.getBucket();

        log.debug("Storing set event for bucket index {} to replicas {}",
                bucket,
                bucketEntry.getPeers().stream()
                        .map(Peer::getHost)
                        .collect(Collectors.joining(",")));

        this.merkle.set(keyHash, hashValue, bucket);

        return bucket;
    }

    @Override
    public int delete(long keyHash) {
        // check to see if leaf exists in merkle tree before we delete? maybe domt need to do anything?
        BucketReplicasEntry bucketEntry = this.hashRing.getReplicasForKey(keyHash);
        if (bucketEntry == null)
            throw new IllegalStateException("Bucket replicas for " + keyHash + " not found?!");

        if (!bucketEntry.getPeers().stream().anyMatch(p -> p.isSelf()))
            return -1;

        int bucket = bucketEntry.getBucket();

        log.debug("Storing delete event for bucket index {} to replicas {}",
                bucket,
                bucketEntry.getPeers().stream()
                        .map(Peer::getHost)
                        .collect(Collectors.joining(",")));

        SimpleLeaf leaf = this.merkle.delete(keyHash, bucket);
        if (leaf == null) {
            log.debug("Delete for key that didnt exist, skipping replication of nonexistent key");
            return 0;
        }

        return bucket;

        // send k,v,timestamp to replica(s)
        // replication event here to other bucket replica(s)
    }

    @Override
    public CompletableFuture<Void> stop() {
        return this.peerDiscovery.shutdown().thenRun(() -> {
            log.info("Peer discovery service closed, unregistered self.");
        }).thenRun(() -> {
            log.debug("Awaiting some time to provide servers to process");

            // do things to rebalance out data here
        });
    }

    public Map<Integer, List<Node>> getBucketReplicasMap() {
        Map<Integer, List<Node>> localBucketMapping = new HashMap<>();

        this.hashRing.getBucketPeers().forEach(bp -> {
            List<Node> nodesForBucket = localBucketMapping.computeIfAbsent(bp.getBucketId(),
                    k -> new ArrayList<>());

            // TODO will we need a separate node ID, or is the host sufficient?
            nodesForBucket.addAll(bp.getReplicas().stream().map(rep
                    -> new Node(rep.getHost(), rep.getHost())).toList());
        });

        return localBucketMapping;
    }

}

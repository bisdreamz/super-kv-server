package org.reset.replication;

import com.nimbus.net.Node;
import net.openhft.hashing.LongHashFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.datastore.DataStore;
import org.reset.replication.discovery.Peer;
import org.reset.replication.discovery.StaticPeerDiscovery;
import org.reset.replication.hashring.BucketReplicasEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ReplicationManager {

    private static final Logger log = LogManager.getLogger(ReplicationManager.class);

    // TODO
    // Disruptor queue and bulk transfer of commands to replica nodes
    // Rebalancing
    // Discovery of peer nodes, in particular the peer bucket replica(s) if any
    //
    // Should handle snapshotting of data for replication
    private final DataStore dataStore;
    private final LongHashFunction hashFunction;
    private final LocalReplicationCluster localReplicationCluster;
    private final List<ExternalReplicationCluster> externalReplicationClusters;

    public ReplicationManager(DataStore dataStore, LongHashFunction hashFunction, int totalBuckets,
                              int virtualNodesPerServer, int replicationFactor) {
        this.dataStore = dataStore;
        this.hashFunction = hashFunction;

        this.localReplicationCluster = new LocalReplicationCluster(
                new StaticPeerDiscovery(List.of(new Peer("localhost", true))),
                hashFunction,
                totalBuckets,
                virtualNodesPerServer,
                replicationFactor);

        this.externalReplicationClusters = new ArrayList<>();
    }

    private boolean hasReplicaPeers(BucketReplicasEntry entry) {
        if (entry == null)
            throw new NullPointerException("Cant't replicate a null entry");

        if (entry.getPeers() == null || entry.getPeers().isEmpty())
            throw new IllegalArgumentException("Peer list is bucket replicas was null or empty!");

        if (entry.getPeers().size() == 1 && entry.getPeers().getFirst().isSelf())
            return false;

        return true;
    }

    public CompletableFuture<Void> start() {
        // start local and external replica clusters
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        CompletableFuture<Void> localFut = this.localReplicationCluster.start().thenRun(() -> {
            log.info("Local replication service started");
        });

        futures.add(localFut);

        this.externalReplicationClusters.forEach(externalReplicationCluster -> {
            CompletableFuture<Void> externalFut = externalReplicationCluster.start().thenRun(() -> {
               log.info("External replication service started for cluster {}",
                       externalReplicationCluster.getClusterHost());
            });

            futures.add(externalFut);
        });

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
           log.info("All replicateion services ready");
        });
    }

    public CompletableFuture<Void> stop() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        CompletableFuture<Void> localFut = this.localReplicationCluster.stop().thenRun(() -> {
            log.info("Local replication service stopped");
        });

        futures.add(localFut);

        this.externalReplicationClusters.forEach(externalReplicationCluster -> {
            CompletableFuture<Void> externalFut = externalReplicationCluster.stop().thenRun(() -> {
                log.info("External replication service stopped for cluster {}",
                        externalReplicationCluster.getClusterHost());
            });

            futures.add(externalFut);
        });

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
            log.info("All replicateion services shutdown");
        });
    }

    /**
     * Sets or updates a key value for replication and synchronization with the local
     * cluster and external(s) if applicable.
     * @param keyHash
     * @param keyData
     * @param valueData
     * @return true if successful, false if this key is determined to belong to
     * different servers!
     */
    public boolean keyUpdate(long keyHash, byte[] keyData, byte[] valueData) {
        long valueHash = this.hashFunction.hashBytes(valueData);

        System.out.println("keyUpdate set");
        int localBucket = this.localReplicationCluster.set(keyHash, valueHash);
        if (localBucket == -1) {
            log.warn("Got insert for key which doesnt belong to our server");
            return false;
        }
        this.externalReplicationClusters.forEach(externalReplicationCluster -> {
            externalReplicationCluster.set(keyHash, valueHash);
        });

        return true;

        // TODO add to replication queue, including timestamp from leaf to ensure consistency
    }

    public boolean keyDelete(long keyHash) {
        int localBucket = this.localReplicationCluster.delete(keyHash);
        this.externalReplicationClusters.forEach(externalReplicationCluster -> {
            externalReplicationCluster.delete(keyHash);
        });

        if (localBucket == -1) {
            log.warn("Got delete for key which doesnt belong to our server");
            return false;
        }

        log.debug("Deleted key with hash {}", keyHash);

        return true;
    }

    public Map<Integer, List<Node>> getLocalBucketMapping() {
        return this.localReplicationCluster.getBucketReplicasMap();
    }

    private void sync() {
        // we initiate this when we feel like our server needs data?
        // dont run if another job or rebalance job running

        // need to ask other random replica server from each of our buckets
        // to send us data we are missing. Perhaps we send them a request
        // which includes the timestamp, bucket index, and hash value
        // and then they can directly respond with the data we are missing?

        // need to track how much data entries were actually missing or stale
    }

}

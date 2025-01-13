package org.reset.datastore;

import net.openhft.hashing.LongHashFunction;
import org.reset.replication.ReplicationManager;

import java.util.concurrent.CompletableFuture;

public class ReplicatingDataStore {

    public static enum OperationStatus {
        SUCCESS,
        DATA_NOT_OURS,
        DATA_NOT_EXIST
    }

    private final DataStore dataStore;
    private final ReplicationManager replicationManager;
    private final LongHashFunction hashFunction;

    /**
     * Construct a replicating datastore with the provided backing datastore.
     * @param dataStore Backing {@link DataStore} implementation
     * @param hashFunction {@link LongHashFunction} Hash function to for keys and data
     * @param bucketSz Count of buckets to split data into. Affects hashring/merkle tree sizes
     * @param virtualNodesPerServer Virtual nodes per server on the hash ring
     * @param replFactor Replication factor, eg 2 means to keep 2 copies of data in each cluster
     */
    public ReplicatingDataStore(DataStore dataStore, LongHashFunction hashFunction,
                                int bucketSz, int virtualNodesPerServer, int replFactor) {
        if (bucketSz < 256 || bucketSz > 8192)
            throw new IllegalArgumentException("Bucket size must be between 256 and 8192");

        if (virtualNodesPerServer < 8 || virtualNodesPerServer > 128)
            throw new IllegalArgumentException("Hash ring virtual node count must be between 8 and 128");

        if (replFactor < 1 || replFactor > 4)
            throw new IllegalArgumentException("Replication factor must be between 1 and 4");

        this.dataStore = dataStore;
        this.replicationManager = new ReplicationManager(dataStore, hashFunction, bucketSz,
                virtualNodesPerServer, replFactor);
        this.hashFunction = hashFunction;
    }

    /**
     * Contstruct a data store which replication functionality disabled
     * @param dataStore Backing {@link DataStore} implementation
     * @param hashFunction {@link LongHashFunction} Hash function to for keys and data
     */
    public ReplicatingDataStore(DataStore dataStore, LongHashFunction hashFunction) {
        this.dataStore = dataStore;
        this.hashFunction = hashFunction;
        this.replicationManager = null;
    }

    public CompletableFuture<Void> start() {
        return replicationManager.start();
    }

    private long hashFor(byte[] data) {
        return hashFunction.hashBytes(data);
    }

    public OperationStatus put(byte[] keyData, byte[] valueData) {
        long keyHash = hashFor(keyData);

        if(this.replicationManager.keyUpdate(keyHash, keyData, valueData) == false)
            return OperationStatus.DATA_NOT_OURS;

        this.dataStore.put(keyHash, keyData, valueData);

        return OperationStatus.SUCCESS;
    }

    public byte[] get(byte[] keyData) {
        return this.dataStore.get(hashFor(keyData));
    }

    public OperationStatus remove(byte[] keyData) {
        long keyHash = hashFor(keyData);

        if(this.replicationManager.keyDelete(keyHash) == false)
            return OperationStatus.DATA_NOT_OURS;

        return this.dataStore.remove(keyHash) ? OperationStatus.SUCCESS
                : OperationStatus.DATA_NOT_EXIST;
    }

    public CompletableFuture<Void> shutdown() {
        return this.dataStore.shutdown();
    }

    public DataStore getDataStore() {
        return this.dataStore;
    }

    public ReplicationManager getReplicationManager() {
        return this.replicationManager;
    }

}

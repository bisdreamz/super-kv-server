package org.reset.replication.merkle;

import net.openhft.hashing.LongHashFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleMerkle {

    private SimpleNode[] buckets;
    private final int bucketSz;

    public SimpleMerkle(int bucketSz, LongHashFunction hashFunction) {
        this.buckets = new SimpleNode[bucketSz];

        for (int i = 0; i < bucketSz; i++)
            this.buckets[i] = new SimpleNode(hashFunction);

        this.bucketSz = bucketSz;
    }

    //private int bucket(long hash) {
    //    return (int) Math.abs(hash % bucketSz);
    //}

    public SimpleLeaf set(long keyHash, long valueHash, int bucketIdx) {
        SimpleNode bucket = this.buckets[bucketIdx];

        return bucket.set(keyHash, valueHash);
    }

    public SimpleLeaf delete(long keyHash, int bucketIdx) {
        SimpleNode bucket = this.buckets[bucketIdx];

        return bucket.delete(keyHash);
    }

    /**
     * Checks to see if the contents of the provided bucket index equal the expected
     * contents by way of hash comparison
     * @param bucket hash bucket index
     * @param hash expected aggregate bucket hash which represents state of all
     *             data within the bucket
     * @return true if they are equal
     */
    public boolean compare(int bucket, long hash) {
        return this.buckets[bucket] != null && this.buckets[bucket].hash() == hash;
    }

    public long getBucketHash(int bucket) {
        return this.buckets[bucket].hash();
    }
    /**
     * Retrieves the current state of all buckets.
     * Useful for replication purposes.
     *
     * @return A map of bucket index to its corresponding SimpleNode.
     */
    public Map<Integer, SimpleNode> getBuckets() {
        Map<Integer, SimpleNode> bucketMap = new ConcurrentHashMap<>();

        for (int i = 0; i < bucketSz; i++) {
            bucketMap.put(i, this.buckets[i]);

        }
        return bucketMap;
    }

}

package org.reset.replication.merkle;

import net.openhft.hashing.LongHashFunction;

public class SimpleMerkle {

    private final LongHashFunction hashFunction;
    private SimpleNode[] buckets;
    private final int bucketSz;

    public SimpleMerkle(int bucketSz, LongHashFunction hashFunction) {
        this.hashFunction = hashFunction;
        this.buckets = new SimpleNode[bucketSz];

        for (int i = 0; i < bucketSz; i++)
            this.buckets[i] = new SimpleNode(hashFunction);

        this.bucketSz = bucketSz;
    }

    private int bucket(long hash) {
        return (int) Math.abs(hash % bucketSz);
    }

    public SimpleLeaf set(byte[] key, byte[] value) {
        long keyHash = hashFunction.hashBytes(key);
        long valueHash = hashFunction.hashBytes(value);

        int bucketIdx = bucket(keyHash);

        SimpleNode bucket = this.buckets[bucketIdx];

        return bucket.set(keyHash, valueHash);
    }

    public SimpleLeaf delete(byte[] key) {
        long keyHash = hashFunction.hashBytes(key);

        int bucketIdx = bucket(keyHash);

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

}

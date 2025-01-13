package org.reset.replication.merkle;

import net.openhft.hashing.LongHashFunction;
import net.openhft.hashing.LongTupleHashFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleNode {

    private final LongHashFunction hashFunction;

    private long hash;
    private long lastModified;
    private ConcurrentHashMap<Long, SimpleLeaf> children;

    public SimpleNode(LongHashFunction hashFunction) {
        this.hashFunction = hashFunction;

        this.lastModified = System.currentTimeMillis();
        this.children = new ConcurrentHashMap<>();
    }

    private synchronized void updateHash(long incHash, long timestamp) {
        this.lastModified = timestamp;

        if (children.isEmpty()) {
            this.hash = 0L;
            return;
        }

        // if history differs, even if current data is the same will the hash comparisons pass?
        // e.g. if a new node comes up and receives all data perfectly from another server,
        // will the summaries still be the same?

        this.hash += incHash;
    }

    public SimpleLeaf set(long keyHash, long valueHash) {
        long timestamp = System.currentTimeMillis();

        SimpleLeaf newLeaf = new SimpleLeaf(valueHash, timestamp);
        SimpleLeaf oldLeaf = children.put(keyHash, newLeaf);

        long hashAdjustment = 0l;
        if (oldLeaf != null) {
            if (oldLeaf.isTombstone()) {
                // If the old entry was deleted, we need to add both key and value hash
                hashAdjustment = keyHash + valueHash;
            } else {
                // If updating an existing entry, just adjust by the difference in value hashes
                hashAdjustment = valueHash - oldLeaf.getHash();
            }
        } else {
            // For a completely new entry, add both key and value hash
            hashAdjustment = keyHash + valueHash;
        }

        // If key existed already, adjust hash summary only by the hash distance
        // in the values to ensure even if re-added without the old value and only
        // the new value that the hash sum would be equal
        //long hashAdjustment = oldLeaf != null ? (valueHash - oldLeaf.getHash())
        //        : (keyHash + valueHash);

        // Ensure hash summary considers value of data as well and
        // not simply the presence of a key
        this.updateHash(hashAdjustment, timestamp);

        return newLeaf;
    }

    public SimpleLeaf delete(long keyHash) {
        long timestamp = System.currentTimeMillis();

        SimpleLeaf oldLeaf = children.get(keyHash);
        SimpleLeaf newLeaf = new SimpleLeaf(oldLeaf != null ? oldLeaf.getHash() : 0l,
                timestamp, true);

        if (oldLeaf == null || oldLeaf.isTombstone())
            return null; // wasnt present or already deleted so nothing to do

        children.put(keyHash, newLeaf);

        // Remove the sum of the key+value hash from the hash summary
        long hashAdjustment = keyHash + oldLeaf.getHash();
        this.updateHash(-hashAdjustment, timestamp);

        return newLeaf;
    }

    public long lastModified() {
        return lastModified;
    }

    public long hash() {
        return hash;
    }

}

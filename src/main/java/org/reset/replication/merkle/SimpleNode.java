package org.reset.replication.merkle;

import net.openhft.hashing.LongHashFunction;

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

    private void updateHash(long timestamp) {
        this.lastModified = timestamp;

        if (children.isEmpty()) {
            this.hash = 0L;
            return;
        }

        int size = children.size();
        long[] hashes = new long[size * 2];

        int index = 0;
        for (Map.Entry<Long, SimpleLeaf> entry : children.entrySet()) {
            hashes[index++] = entry.getKey();
            hashes[index++] = entry.getValue().getHash();
        }

        this.hash = hashFunction.hashLongs(hashes);
    }

    public SimpleLeaf set(long keyHash, long valueHash) {
        long timestamp = System.currentTimeMillis();

        SimpleLeaf newLeaf = new SimpleLeaf(valueHash, timestamp);
        SimpleLeaf oldLeaf = children.put(keyHash, newLeaf);

        if (oldLeaf != null && oldLeaf.getHash() == newLeaf.getHash())
            return oldLeaf; // no actual data change, avoid recalculation

        synchronized (this) {
            this.updateHash(timestamp);
        }

        return newLeaf;
    }

    public SimpleLeaf delete(long keyHash) {
        long timestamp = System.currentTimeMillis();

        SimpleLeaf oldLeaf = children.get(keyHash);
        SimpleLeaf newLeaf = new SimpleLeaf(oldLeaf != null ? oldLeaf.getHash() : 0l,
                timestamp, true);

        children.put(keyHash, newLeaf);

        if (oldLeaf == null)
            return null; // wasnt present so nothing to do

        synchronized (this) {
            this.updateHash(timestamp);
        }

        return newLeaf;
    }

    public long lastModified() {
        return lastModified;
    }

    public long hash() {
        return hash;
    }

}

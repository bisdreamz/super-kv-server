package org.reset.datastore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class HashMapDataStore implements DataStore {

    private final ConcurrentHashMap<Long, byte[]> map;

    public HashMapDataStore() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public boolean supportsExpiryAfterWrite() {
        return false;
    }

    @Override
    public boolean supportsExpiryAfterRead() {
        return false;
    }

    @Override
    public boolean supportsPersistence() {
        return false;
    }

    @Override
    public void put(long keyHash, byte[] keyData, byte[] valueData) {
        this.map.put(keyHash, valueData);
    }

    @Override
    public byte[] get(long keyHash) {
        return this.map.get(keyHash);
    }

    @Override
    public boolean remove(long keyHash) {
        return this.map.remove(keyHash) != null;
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

}

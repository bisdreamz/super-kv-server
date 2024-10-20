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
    public void put(long key, byte[] value) {
        this.map.put(key, value);
    }

    @Override
    public byte[] get(long key) {
        return this.map.get(key);
    }

    @Override
    public boolean remove(long key) {
        return this.map.remove(key) != null;
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

}

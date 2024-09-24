package org.reset.datastore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class HashMapDataStore implements DataStore {

    private final ConcurrentHashMap<String, byte[]> map;

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
    public void put(byte[] key, byte[] value) {
        this.map.put(new String(key), value);
    }

    @Override
    public byte[] get(byte[] key) {
        return this.map.get(new String(key));
    }

    @Override
    public boolean remove(byte[] key) {
        return this.map.remove(new String(key)) != null;
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

}

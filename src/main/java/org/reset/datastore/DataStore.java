package org.reset.datastore;

import java.util.concurrent.CompletableFuture;

public interface DataStore {

    public boolean supportsExpiryAfterWrite();

    public boolean supportsExpiryAfterRead();

    public boolean supportsPersistence();

    /**
     * Insert data
     * @param key
     * @param value
     */
    public void put(byte[] key, byte[] value);

    /**
     * Get data
     * @param key
     * @return
     */
    public byte[] get(byte[] key);

    /**
     * Remove datta
     * @param key
     * @return boolean true if successful, false if known unrecognized key
     */
    public boolean remove(byte[] key);

    /**
     * Close and complete and shutdown needed to persist data or
     * other cleanup routines
     * @return CompletableFuture which completes upon shutdown
     */
    public CompletableFuture<Void> shutdown();

}

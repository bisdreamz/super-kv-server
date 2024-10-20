package org.reset.datastore;

import java.util.concurrent.CompletableFuture;

public interface DataStore {

    public boolean supportsExpiryAfterWrite();

    public boolean supportsExpiryAfterRead();

    public boolean supportsPersistence();

    /**
     * Insert data
     * @param key hash of the key
     * @param value
     */
    public void put(long key, byte[] value);

    /**
     * Get data
     * @param key hash of key
     * @return byte array of data null if not present
     */
    public byte[] get(long key);

    /**
     * Remove datta
     * @param key hash of key
     * @return boolean true if successful, false if known unrecognized key
     */
    public boolean remove(long key);

    /**
     * Close and complete and shutdown needed to persist data or
     * other cleanup routines
     * @return CompletableFuture which completes upon shutdown
     */
    public CompletableFuture<Void> shutdown();

}

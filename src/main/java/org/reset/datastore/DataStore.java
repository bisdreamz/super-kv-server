package org.reset.datastore;

import java.util.concurrent.CompletableFuture;

public interface DataStore {

    public boolean supportsExpiryAfterWrite();

    public boolean supportsExpiryAfterRead();

    public boolean supportsPersistence();

    /**
     * Insert data
     * @param keyHash hash of the key
     * @param valueData
     */
    public void put(long keyHash, byte[] keyData, byte[] valueData);

    /**
     * Get data
     * @param keyHash hash of key
     * @return byte array of data null if not present
     */
    public byte[] get(long keyHash);

    /**
     * Remove datta
     * @param keyHash hash of key
     * @return boolean true if successful, false if known unrecognized key
     */
    public boolean remove(long keyHash);

    /**
     * Close and complete and shutdown needed to persist data or
     * other cleanup routines
     * @return CompletableFuture which completes upon shutdown
     */
    public CompletableFuture<Void> shutdown();

}

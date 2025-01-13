package org.reset.replication;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractReplicationCluster {

    public abstract CompletableFuture<Void> start();

    public abstract int set(long keyHash, long hashValue);

    public abstract int delete(long keyHash);

    public abstract CompletableFuture<Void> stop();

    public CompletableFuture<Void> sync() {
        // begin sync process to other servers
        // matching the same data buckets as we are responsible for
        // and filtering out node self

        // local uses hashring
        // external uses bucket map api response

        return CompletableFuture.completedFuture(null);
    }

}

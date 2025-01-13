package org.reset;

import com.nimbus.routing.HashConstants;
import net.openhft.hashing.LongHashFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.datastore.DataStore;
import org.reset.datastore.ReplicatingDataStore;
import org.reset.server.NettyProtoServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        final LongHashFunction hashFunction = HashConstants.HASH_FUNCTION;
        final SuperConfig config = new SuperConfig();
        final DataStore dataStore = DataStores.getDataStore(config);

        final ReplicatingDataStore replicatingDataStore = new ReplicatingDataStore(dataStore,
                hashFunction,
                config.getHashringTotalBuckets(),
                config.getHashringVirtualNodesPerServer(),
                config.getReplicas());

        final CommandHandler handler = new CommandHandler(replicatingDataStore);

        final NettyProtoServer server = new NettyProtoServer(8080, buffer
                -> handler.process(buffer));

        replicatingDataStore.start()
                .thenRun(() -> log.info("Replication started"))
                .thenCompose(v -> {
                    try {
                        return server.start();
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                }).thenRun(() -> log.info("Super-KV started"))
                .exceptionally(e -> {
                    log.error("Error starting server", e);

                    throw new RuntimeException(e);
                });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");

            server.shutdown().thenRun(() -> log.info("Data listener stopped, starting replication shutdown.."))
                    .thenRun(() -> replicatingDataStore.shutdown())
                    .thenRun(() -> log.info("Super-KV shutdown complete"))
                    .thenRun(() -> latch.countDown());
        }));

        latch.await();
    }

}
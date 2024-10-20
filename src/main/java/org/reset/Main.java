package org.reset;

import com.nimbus.routing.HashConstants;
import net.openhft.hashing.LongHashFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.datastore.DataStore;
import org.reset.replication.ReplicationManager;
import org.reset.replication.merkle.SimpleMerkle;
import org.reset.server.NettyProtoServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        final LongHashFunction hashFunction = HashConstants.HASH_FUNCTION;
        final SuperConfig config = new SuperConfig();
        final DataStore dataStore = DataStores.getDataStore(config);
        final CommandHandler handler = new CommandHandler(dataStore, hashFunction);
        final ReplicationManager replicationManager = new ReplicationManager(hashFunction,
                config.getHashringTotalBuckets(), config.getHashringVirtualNodesPerServer(),config.getReplicas());

        final NettyProtoServer server = new NettyProtoServer(8080, buffer
                -> handler.process(buffer));

        replicationManager.start()
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
                    .thenRun(() -> replicationManager.stop())
                    .thenRun(() -> log.info("Super-KV shutdown complete"))
                    .thenRun(() -> latch.countDown());
        }));

        latch.await();
    }

}
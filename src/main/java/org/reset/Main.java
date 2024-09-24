package org.reset;

import org.reset.datastore.DataStore;
import org.reset.server.NettyProtoServer;

import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        final DataStore dataStore = DataStores.getDataStore(new SuperConfig());

        final CommandHandler handler = new CommandHandler(dataStore);

        final NettyProtoServer server = new NettyProtoServer(8080, buffer
                -> handler.process(buffer));

        server.start().thenRun(() -> {
            System.out.println("Server started");
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            server.shutdown().thenRun(latch::countDown);
        }));

        latch.await();
    }

}
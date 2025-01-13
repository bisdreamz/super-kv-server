package org.reset.replication;

import com.nimbus.net.client.SuperTcpClient;
import com.nimbus.proto.messages.RequestMessage;
import com.nimbus.proto.messages.ResponseMessage;
import com.nimbus.proto.protocol.RequestProtocol;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reset.replication.discovery.Peer;
import org.reset.replication.hashring.BucketReplicasEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalReplicationCluster extends AbstractReplicationCluster {

    private static final Logger log = LogManager.getLogger(ExternalReplicationCluster.class);

    private final String clusterHost;
    private final int clusterPort;
    // Map of bucket idx -> replicas per bucket, retrieved from external cluster
    private final Map<Integer, BucketReplicasEntry> bucketMapping;
    // Client connections to each node in the external cluster
    private final Map<String, SuperTcpClient> clients;

    public ExternalReplicationCluster(String clusterHost, int clusterPort) {
        this.clusterHost = clusterHost;
        this.clusterPort = clusterPort;

        this.bucketMapping = new ConcurrentHashMap<>();
        this.clients = new ConcurrentHashMap<>();
    }

    /**
     * Connect to external cluster by generic cluster hostname, and
     * retrieve the initial bucket -> node mapping
     * @param clusterHost
     * @param port
     * @return
     */
    private CompletableFuture<List<BucketReplicasEntry>> getBucketMapping(String clusterHost, int port) {
        final SuperTcpClient client = new SuperTcpClient(clusterHost, port, 1, 5000, Collections.emptyMap());

        try {
            RequestMessage requestMessage = new RequestMessage(PooledByteBufAllocator.DEFAULT.buffer());

            requestMessage.command(RequestProtocol.REPL_CMD_BUCKET_MAPPING);
            return client.send(requestMessage.end()).thenCompose(res -> {
                ResponseMessage responseMessage = new ResponseMessage(res);

                // parse response however itll be written

                requestMessage.release();

                return CompletableFuture.completedFuture(
                        List.of(new BucketReplicasEntry(List.of(new Peer(clusterHost, false)), 1)));
            });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> start() {
        // get initial node mapping then establish initial connections to external cluster nodes
        return this.getBucketMapping(this.clusterHost, this.clusterPort).handle((buckets, ex) -> {
            log.info("Got list of buckets from external cluster, establishing direct node connections..");

            // build cache of bucketidx -> peer
            // load clients cache with new supertcp clients

            return null;
        });
    }

    @Override
    public void set(long keyHash, long valueHash) {

    }

    @Override
    public void delete(long keyHash) {

    }

    @Override
    public CompletableFuture<Void> stop() {
        return null;
    }

    public String getClusterHost() {
        return clusterHost;
    }
}

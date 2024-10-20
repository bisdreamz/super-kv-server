package org.reset.replication;

import net.openhft.hashing.LongHashFunction;
import org.reset.replication.discovery.BucketPeer;
import org.reset.replication.discovery.Peer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the consistent hash ring, bucket assignments, and replica distributions.
 * Optimized for fast, low-garbage lookups and graceful handling of server failures.
 */
public class ConsistentHashRing {
    private final SortedMap<Long, Peer> hashRing;
    private final int virtualNodesPerServer;
    private final int replicationFactor;
    private final int totalBuckets;
    private final LongHashFunction hashFunction;
    private final List<BucketPeer> bucketPeers;
    private final Map<String, List<Peer>> bucketToReplicasMap;
    private final Map<String, List<Integer>> serverToBucketsMap;

    /**
     * Construct a ConsistentHashRing for managing routing of keys to different data buckets,
     * and assisting in re-assigning or re-balancing buckets when servers come up or go down.
     * @param totalBuckets Total data buckets to split keyspace into, higher value means
     *                     more to compare during integrity checks but improved distribution of data
     * @param virtualNodesPerServer
     * @param replicationFactor
     * @param hashFunction
     */
    public ConsistentHashRing(int totalBuckets, int virtualNodesPerServer, int replicationFactor,
                              LongHashFunction hashFunction) {
        this.hashRing = new TreeMap<>();
        this.bucketPeers = new ArrayList<>();
        this.bucketToReplicasMap = new ConcurrentHashMap<>();
        this.serverToBucketsMap = new ConcurrentHashMap<>();

        this.virtualNodesPerServer = virtualNodesPerServer;
        this.replicationFactor = replicationFactor;
        this.totalBuckets = totalBuckets;
        this.hashFunction = hashFunction;
    }

    /**
     * Adds a server along with its virtual nodes to the hash ring.
     *
     * @param server Peer instance representing the server.
     */
    public void addServer(Peer server) {
        for (int i = 0; i < virtualNodesPerServer; i++) {
            String virtualNodeId = generateVirtualNodeId(server.getHost(), i);
            long hash = hashFunction.hashChars(virtualNodeId);
            hashRing.put(hash, server);
        }

        serverToBucketsMap.putIfAbsent(server.getHost(), new ArrayList<>());
        this.assignBuckets();
    }

    /**
     * Removes a server and its virtual nodes from the hash ring.
     *
     * @param server Peer instance representing the server to remove.
     */
    public void removeServer(Peer server) {
        for (int i = 0; i < virtualNodesPerServer; i++) {
            String virtualNodeId = generateVirtualNodeId(server.getHost(), i);
            long hash = hashFunction.hashChars(virtualNodeId);
            hashRing.remove(hash);
        }

        serverToBucketsMap.remove(server.getHost());
        this.assignBuckets();
    }

    /**
     * Generates a unique identifier for a virtual node.
     *
     * @param serverHost Host identifier of the server.
     * @param index      Index of the virtual node.
     * @return Unique virtual node identifier.
     */
    private String generateVirtualNodeId(String serverHost, int index) {
        return serverHost + "-VN" + index;
    }

    /**
     * Assigns all buckets to their respective replicas based on the current hash ring.
     * Also populates the bucket-to-replicas cache.
     */
    private void assignBuckets() {
        bucketPeers.clear();
        bucketToReplicasMap.clear();
        serverToBucketsMap.clear();

        for (String serverHost : serverToBucketsMap.keySet()) {
            serverToBucketsMap.put(serverHost, new ArrayList<>());
        }

        for (int bucketId = 0; bucketId < totalBuckets; bucketId++) {
            String bucketKey = "Bucket-" + bucketId;
            List<Peer> replicas = getReplicas(bucketKey);
            BucketPeer bucketPeer = new BucketPeer(bucketId, replicas);
            bucketPeers.add(bucketPeer);

            // Update bucket-to-replicas cache
            bucketToReplicasMap.put(bucketKey, Collections.unmodifiableList(replicas));

            // Update server to buckets mapping
            for (Peer peer : replicas) {
                serverToBucketsMap.computeIfAbsent(peer.getHost(), k -> new ArrayList<>()).add(bucketId);
            }
        }
    }

    /**
     * Retrieves the list of replica peers for a given bucket key.
     *
     * @param bucketKey The key representing the bucket.
     * @return Unmodifiable list of Peer instances assigned as replicas.
     */
    public List<Peer> getReplicas(String bucketKey) {
        // Check if the bucket's replicas are already cached
        List<Peer> cachedReplicas = bucketToReplicasMap.get(bucketKey);
        if (cachedReplicas != null)
            return cachedReplicas;

        // If not cached, compute the replicas and cache them
        List<Peer> replicas = new ArrayList<>();
        long hash = hashFunction.hashChars(bucketKey);

        if (hashRing.isEmpty())
            return replicas;

        // Tail map from the bucket's hash to the end of the ring
        SortedMap<Long, Peer> tailMap = hashRing.tailMap(hash);
        Iterator<Peer> it = tailMap.values().iterator();

        // Collect replicas, ensuring distinct servers
        while (replicas.size() < replicationFactor && it.hasNext()) {
            Peer peer = it.next();
            if (!replicas.contains(peer)) {
                replicas.add(peer);
            }
        }

        // If not enough replicas found, wrap around the ring
        if (replicas.size() < replicationFactor) {
            for (Peer peer : hashRing.values()) {
                if (replicas.size() >= replicationFactor)
                    break;

                if (!replicas.contains(peer))
                    replicas.add(peer);
            }
        }

        // Cache the computed replicas
        List<Peer> unmodifiableReplicas = Collections.unmodifiableList(replicas);
        bucketToReplicasMap.put(bucketKey, unmodifiableReplicas);

        return unmodifiableReplicas;
    }

    /**
     * Retrieves all BucketPeer instances.
     *
     * @return Unmodifiable list of BucketPeer objects.
     */
    public List<BucketPeer> getBucketPeers() {
        return Collections.unmodifiableList(bucketPeers);
    }

    /**
     * Retrieves the mapping from servers to their assigned buckets.
     *
     * @return Unmodifiable map of server hosts to lists of bucket IDs.
     */
    public Map<String, List<Integer>> getServerToBucketsMap() {
        Map<String, List<Integer>> unmodifiableMap = new HashMap<>();

        for (Map.Entry<String, List<Integer>> entry : serverToBucketsMap.entrySet()) {
            unmodifiableMap.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }

        return Collections.unmodifiableMap(unmodifiableMap);
    }

    /**
     * Retrieves the list of replicas for a given data key.
     * This method maps the data key to its corresponding bucket and returns the replicas.
     *
     * @param dataKey The key representing the data.
     * @return Unmodifiable list of Peer instances assigned as replicas.
     */
    public List<Peer> getReplicasForDataKey(String dataKey) {
        // Determine the bucket for the data key
        int bucketId = determineBucketId(dataKey);
        String bucketKey = "Bucket-" + bucketId;
        return getReplicas(bucketKey);
    }

    /**
     * Determines the bucket ID for a given data key.
     *
     * @param dataKey The key representing the data.
     * @return The bucket ID.
     */
    private int determineBucketId(String dataKey) {
        // Simple hash to bucket mapping using modulo
        long hash = hashFunction.hashChars(dataKey);
        return (int) (hash % totalBuckets);
    }

    /**
     * Identifies all buckets that have the specified server as a replica.
     *
     * @param serverHost The host identifier of the server.
     * @return Set of bucket IDs that are assigned to the server.
     */
    public Set<Integer> getAffectedBucketsOnFailure(String serverHost) {
        Set<Integer> affectedBuckets = new HashSet<>();
        List<Integer> buckets = serverToBucketsMap.get(serverHost);

        if (buckets != null)
            affectedBuckets.addAll(buckets);

        return affectedBuckets;
    }

    /**
     * Reassigns replicas for affected buckets after a server failure.
     *
     * @param failedServerHost The host identifier of the failed server.
     * @return Map of bucket IDs to their new replica peers.
     */
    public Map<Integer, List<Peer>> handleServerFailure(String failedServerHost) {
        // Step 1: Identify affected buckets
        Set<Integer> affectedBuckets = getAffectedBucketsOnFailure(failedServerHost);
        System.out.println("Affected Buckets: " + affectedBuckets.size());

        // Step 2: Remove the failed server from the hash ring
        Peer failedServer = null;
        for (Peer server : hashRing.values()) {
            if (server.getHost().equals(failedServerHost)) {
                failedServer = server;
                break;
            }
        }

        if (failedServer != null) {
            removeServer(failedServer);
        } else {
            System.out.println("Server " + failedServerHost + " not found in the hash ring.");
            return Collections.emptyMap();
        }

        // Step 3: Reassign replicas for affected buckets
        Map<Integer, List<Peer>> reassignedBuckets = new HashMap<>();
        for (Integer bucketId : affectedBuckets) {
            String bucketKey = "Bucket-" + bucketId;
            List<Peer> newReplicas = getReplicas(bucketKey);
            reassignedBuckets.put(bucketId, newReplicas);
        }

        // Step 4: Update bucketPeers and serverToBucketsMap accordingly
        for (Integer bucketId : affectedBuckets) {
            String bucketKey = "Bucket-" + bucketId;
            List<Peer> newReplicas = reassignedBuckets.get(bucketId);
            // Update BucketPeer
            BucketPeer bucketPeer = new BucketPeer(bucketId, newReplicas);
            bucketPeers.set(bucketId, bucketPeer);
            // Update bucketToReplicasMap
            bucketToReplicasMap.put(bucketKey, Collections.unmodifiableList(newReplicas));
        }

        // Step 5: Update serverToBucketsMap for new replicas
        // Clear and rebuild serverToBucketsMap to ensure consistency
        rebuildServerToBucketsMap();

        // Step 6: Return the mapping of bucket IDs to new replicas for data redistribution
        return reassignedBuckets;
    }

    /**
     * Rebuilds the serverToBucketsMap based on current bucketToReplicasMap.
     * This ensures that the mapping remains consistent after reassignments.
     */
    private void rebuildServerToBucketsMap() {
        serverToBucketsMap.clear();
        for (Map.Entry<String, List<Peer>> entry : bucketToReplicasMap.entrySet()) {
            String bucketKey = entry.getKey();
            List<Peer> replicas = entry.getValue();
            int bucketId = Integer.parseInt(bucketKey.split("-")[1]);
            for (Peer peer : replicas) {
                serverToBucketsMap.computeIfAbsent(peer.getHost(), k -> new ArrayList<>()).add(bucketId);
            }
        }
    }

    /**
     * Demonstrates the ConsistentHashRing functionality, including failure handling.
     */
    public static void main(String[] args) {
        // Define peers (servers)
        Peer serverA = new Peer("Server-A", true);
        Peer serverB = new Peer("Server-B", false);
        Peer serverC = new Peer("Server-C", false);
        Peer serverD = new Peer("Server-D", false);
        List<Peer> servers = Arrays.asList(serverA, serverB, serverC, serverD);

        // Configuration
        int virtualNodesPerServer = 64; // 64 virtual nodes per server
        int replicationFactor = 2;      // 2 replicas per bucket
        int totalBuckets = 1024;        // 1024 total buckets

        // Initialize ConsistentHashRing
        ConsistentHashRing hashRing = new ConsistentHashRing(
                virtualNodesPerServer, replicationFactor, totalBuckets, LongHashFunction.xx3());

        // Display bucket assignments for the first 20 buckets
        System.out.println("Initial Bucket Assignments (First 20 Buckets):");
        for (int i = 0; i < 20; i++) {
            BucketPeer bucketPeer = hashRing.getBucketPeers().get(i);
            System.out.println(bucketPeer);
        }

        // Display replica distribution across servers
        System.out.println("\nReplica Distribution Across Servers:");
        Map<String, List<Integer>> serverToBuckets = hashRing.getServerToBucketsMap();
        for (Peer server : servers) {
            List<Integer> assignedBuckets = serverToBuckets.getOrDefault(server.getHost(), Collections.emptyList());
            System.out.println(server.getHost() + " is responsible for " + assignedBuckets.size() + " replicas.");
        }

        // Simulate server failure
        String failedServerHost = "Server-C";
        System.out.println("\n--- Simulating Failure of " + failedServerHost + " ---\n");
        Map<Integer, List<Peer>> reassignedBuckets = hashRing.handleServerFailure(failedServerHost);

        // Display bucket assignments after failure for the first 20 buckets
        System.out.println("Bucket Assignments After Failure of " + failedServerHost + " (First 20 Buckets):");
        for (int i = 0; i < 20; i++) {
            BucketPeer bucketPeer = hashRing.getBucketPeers().get(i);
            System.out.println(bucketPeer);
        }

        // Display updated replica distribution across remaining servers
        System.out.println("\nReplica Distribution Across Remaining Servers:");
        serverToBuckets = hashRing.getServerToBucketsMap();
        List<Peer> remainingServers = new ArrayList<>(servers);
        remainingServers.removeIf(server -> server.getHost().equals(failedServerHost));
        for (Peer server : remainingServers) {
            List<Integer> assignedBuckets = serverToBuckets.getOrDefault(server.getHost(), Collections.emptyList());
            System.out.println(server.getHost() + " is responsible for " + assignedBuckets.size() + " replicas.");
        }

        // Example client lookup
        String dataKey = "user1234";
        List<Peer> replicas = hashRing.getReplicasForDataKey(dataKey);
        System.out.println("\nReplicas for data key '" + dataKey + "': " + replicas);
    }
}
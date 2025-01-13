package org.reset.replication.hashring;

import com.nimbus.routing.HashConstants;
import net.openhft.hashing.LongHashFunction;
import org.reset.replication.discovery.BucketPeer;
import org.reset.replication.discovery.Peer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private final Map<String, BucketReplicasEntry> bucketToReplicasMap;
    private final Map<String, List<Integer>> serverToBucketsMap;

    /**
     * Construct a ConsistentHashRing for managing routing of keys to different data buckets,
     * and assisting in re-assigning or re-balancing buckets when servers come up or go down.
     * @param totalBuckets Total data buckets to split keyspace into, higher value means
     *                     more to compare during integrity checks but improved distribution of data
     * @param virtualNodesPerServer Number of virtual nodes per physical server
     * @param replicationFactor Number of replicas to maintain for each bucket
     * @param hashFunction Hash function to use for consistent hashing
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
     * Primary method for getting bucket and replica information for a key
     * @param precomputedHash the precomputed hash of the key
     * @return BucketReplicasEntry containing bucket ID and replica peers
     */
    public BucketReplicasEntry getReplicasForKey(long precomputedHash) {
        int bucketId = getBucketFromHash(precomputedHash);
        String bucketKey = "Bucket-" + bucketId;
        return getReplicas(bucketKey);
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
     * Removes a server from the ring and returns affected bucket reassignments.
     * This handles both planned removals and failure scenarios.
     *
     * @param server Peer instance representing the server to remove.
     * @return Map of affected bucket IDs to their new replica assignments
     */
    public Map<Integer, DataMovementPlan> removeServer(Peer server) {
        // Get current assignments before removal
        Map<Integer, Set<Peer>> currentHolders = new HashMap<>();
        Set<Integer> affectedBuckets = new HashSet<>();

        // Capture which peers currently hold each bucket's data
        // AND capture affected buckets before we clear anything
        for (BucketPeer bucketPeer : getBucketPeers()) {
            Set<Peer> replicas = new HashSet<>(bucketPeer.getReplicas());
            currentHolders.put(bucketPeer.getBucketId(), replicas);

            // If this bucket has the server we're removing, it's affected
            if (replicas.contains(server)) {
                affectedBuckets.add(bucketPeer.getBucketId());
            }
        }

        // Remove server from ring
        for (int i = 0; i < virtualNodesPerServer; i++) {
            String virtualNodeId = generateVirtualNodeId(server.getHost(), i);
            long hash = hashFunction.hashChars(virtualNodeId);
            hashRing.remove(hash);
        }

        serverToBucketsMap.remove(server.getHost());

        // Reassign all buckets (this updates virtual node mappings)
        this.assignBuckets();

        // Create movement plans for affected buckets
        Map<Integer, DataMovementPlan> movementPlans = new HashMap<>();

        for (int bucketId : affectedBuckets) {  // Use our captured set instead of getAffectedBuckets
            // Get original holders minus the removed server
            Set<Peer> originalHolders = new HashSet<>(currentHolders.get(bucketId));
            originalHolders.remove(server);

            // Get new assignments after virtual node remapping
            BucketPeer newBucketPeer = getBucketPeers().get(bucketId);
            Set<Peer> newAssignees = new HashSet<>(newBucketPeer.getReplicas());

            // Find which of the new assignees don't already have the data
            Set<Peer> peersNeedingData = new HashSet<>(newAssignees);
            peersNeedingData.removeAll(originalHolders);

            // Always create a plan for affected buckets
            movementPlans.put(bucketId, new DataMovementPlan(
                    bucketId,
                    originalHolders,
                    peersNeedingData
            ));
        }

        return movementPlans;
    }

    /**
     * Gets all buckets that would be affected by removing a server
     * @param server The server to check
     * @return Set of affected bucket IDs
     */
    public Set<Integer> getAffectedBuckets(Peer server) {
        Set<Integer> affectedBuckets = new HashSet<>();
        List<Integer> buckets = serverToBucketsMap.get(server.getHost());

        if (buckets != null) {
            affectedBuckets.addAll(buckets);
        }

        return affectedBuckets;
    }

    /**
     * Generates a unique identifier for a virtual node.
     *
     * @param serverHost Host identifier of the server.
     * @param index Index of the virtual node.
     * @return Unique virtual node identifier.
     */
    private String generateVirtualNodeId(String serverHost, int index) {
        return serverHost + "-VN" + index;
    }

    /**
     * Gets bucket assignment for a precomputed hash
     */
    private int getBucketFromHash(long precomputedHash) {
        return (int) (Math.abs(precomputedHash) % totalBuckets);
    }

    /**
     * Assigns all buckets to their respective replicas based on the current hash ring.
     * Also populates the bucket-to-replicas cache.
     */
    private void assignBuckets() {
        bucketPeers.clear();
        bucketToReplicasMap.clear();

        // Don't clear serverToBucketsMap until after we've used its keys
        Set<String> currentServers = new HashSet<>(serverToBucketsMap.keySet());
        serverToBucketsMap.clear();

        // Initialize empty lists for all known servers
        for (String serverHost : currentServers) {
            serverToBucketsMap.put(serverHost, new ArrayList<>());
        }

        for (int bucketId = 0; bucketId < totalBuckets; bucketId++) {
            String bucketKey = "Bucket-" + bucketId;
            BucketReplicasEntry replicas = getReplicas(bucketKey);
            BucketPeer bucketPeer = new BucketPeer(bucketId, replicas.getPeers());
            bucketPeers.add(bucketPeer);

            // Update server to buckets mapping
            for (Peer peer : replicas.getPeers()) {
                serverToBucketsMap.computeIfAbsent(peer.getHost(), k -> new ArrayList<>())
                        .add(replicas.getBucket());
            }
        }
    }

    /**
     * Retrieves the list of replica peers for a given bucket key.
     *
     * @param bucketKey The key representing the bucket.
     * @return BucketReplicasEntry containing bucket and replica information
     */
    private BucketReplicasEntry getReplicas(String bucketKey) {
        // Check if the bucket's replicas are already cached
        BucketReplicasEntry cached = bucketToReplicasMap.get(bucketKey);
        if (cached != null) {
            return cached;
        }

        // If not cached, compute the replicas
        List<Peer> replicas = new ArrayList<>();
        long hash = hashFunction.hashChars(bucketKey);
        int bucketId = Integer.parseInt(bucketKey.split("-")[1]);

        if (hashRing.isEmpty()) {
            return new BucketReplicasEntry(Collections.unmodifiableList(replicas), bucketId);
        }

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
                if (replicas.size() >= replicationFactor) {
                    break;
                }
                if (!replicas.contains(peer)) {
                    replicas.add(peer);
                }
            }
        }

        // Create and cache the entry
        BucketReplicasEntry entry = new BucketReplicasEntry(
                Collections.unmodifiableList(replicas),
                bucketId
        );
        bucketToReplicasMap.put(bucketKey, entry);

        return entry;
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
     * Rebuilds the serverToBucketsMap based on current bucketToReplicasMap.
     * This ensures that the mapping remains consistent after reassignments.
     */
    private void rebuildServerToBucketsMap() {
        serverToBucketsMap.clear();
        for (Map.Entry<String, BucketReplicasEntry> entry : bucketToReplicasMap.entrySet()) {
            BucketReplicasEntry replicaEntry = entry.getValue();
            for (Peer peer : replicaEntry.getPeers()) {
                serverToBucketsMap.computeIfAbsent(peer.getHost(), k -> new ArrayList<>())
                        .add(replicaEntry.getBucket());
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
                totalBuckets, virtualNodesPerServer, replicationFactor, LongHashFunction.xx3());

        // Add servers to the ring
        for (Peer server : servers) {
            hashRing.addServer(server);
        }

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

        // Example of handling server removal
        Peer serverToRemove = serverC;
        System.out.println("\n--- Removing " + serverToRemove.getHost() + " ---\n");

        // Get affected buckets before removal
        Set<Integer> willBeAffected = hashRing.getAffectedBuckets(serverToRemove);
        System.out.println("Buckets that will be affected: " + willBeAffected.size());

        // Remove server and get reassignments
        Map<Integer, DataMovementPlan> reassignments = hashRing.removeServer(serverToRemove);

        // Display reassignments
        System.out.println("\nBucket Reassignments:");
        for (Map.Entry<Integer, DataMovementPlan> entry : reassignments.entrySet()) {
            System.out.println("Bucket " + entry.getKey() + " -> " +
                    entry.getValue().getNewRecipients());
        }

        // Example client lookup using precomputed hash
        String dataKey = "user1234";
        long keyHash = HashConstants.HASH_FUNCTION.hashChars(dataKey);
        BucketReplicasEntry replicaInfo = hashRing.getReplicasForKey(keyHash);
        System.out.println("\nFor data key '" + dataKey + "':");
        System.out.println("Bucket: " + replicaInfo.getBucket());
        System.out.println("Replicas: " + replicaInfo.getPeers());
    }
}
package org.reset.replication.hashring;

import com.nimbus.routing.HashConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import net.openhft.hashing.LongHashFunction;
import org.reset.replication.discovery.Peer;
import org.reset.replication.discovery.BucketPeer;

import java.util.*;
import java.util.stream.Collectors;

public class ConsistentHashRingTest {

    private ConsistentHashRing hashRing;
    private List<Peer> servers;
    private static final int VIRTUAL_NODES = 64;
    private static final int REPLICATION_FACTOR = 2;
    private static final int TOTAL_BUCKETS = 1024;

    @BeforeEach
    void setUp() {
        hashRing = new ConsistentHashRing(
                TOTAL_BUCKETS,
                VIRTUAL_NODES,
                REPLICATION_FACTOR,
                HashConstants.HASH_FUNCTION
        );

        servers = Arrays.asList(
                new Peer("Server-A", true),
                new Peer("Server-B", false),
                new Peer("Server-C", false),
                new Peer("Server-D", false)
        );
    }

    @Test
    void testInitialDistribution() {
        // Add all servers
        servers.forEach(hashRing::addServer);

        // Get distribution
        Map<String, List<Integer>> distribution = hashRing.getServerToBucketsMap();

        // Verify each server has buckets assigned
        for (Peer server : servers) {
            List<Integer> buckets = distribution.get(server.getHost());
            assertNotNull(buckets, "Server should have assigned buckets");
            assertFalse(buckets.isEmpty(), "Server should have non-zero buckets");

            // Each server should have roughly equal distribution
            // Allow for some variance but shouldn't be too extreme
            int expectedAverage = (TOTAL_BUCKETS * REPLICATION_FACTOR) / servers.size();
            int allowedVariance = expectedAverage / 2; // 50% variance allowed
            assertTrue(
                    Math.abs(buckets.size() - expectedAverage) <= allowedVariance,
                    String.format("Server %s has %d buckets, expected roughly %d (±%d)",
                            server.getHost(), buckets.size(), expectedAverage, allowedVariance)
            );
        }
    }

    @Test
    void testKeyConsistency() {
        servers.forEach(hashRing::addServer);

        // Test multiple keys get consistent bucket assignments
        String[] testKeys = {"key1", "key2", "key3", "key4", "key5"};
        Map<String, BucketReplicasEntry> firstAssignments = new HashMap<>();

        // Get initial assignments
        for (String key : testKeys) {
            long keyHash = LongHashFunction.xx().hashChars(key);
            BucketReplicasEntry replicas = hashRing.getReplicasForKey(keyHash);
            firstAssignments.put(key, replicas);
        }

        // Get assignments again and verify they're the same
        for (String key : testKeys) {
            long keyHash = LongHashFunction.xx().hashChars(key);
            BucketReplicasEntry replicas = hashRing.getReplicasForKey(keyHash);
            assertEquals(
                    firstAssignments.get(key).getBucket(),
                    replicas.getBucket(),
                    "Bucket assignment should be consistent for the same key"
            );
        }
    }

    @Test
    void testServerRemoval() {
        // Add all servers first
        servers.forEach(hashRing::addServer);

        // Get initial state of all buckets
        Map<String, Set<Integer>> serverBuckets = new HashMap<>();
        for (BucketPeer bucketPeer : hashRing.getBucketPeers()) {
            for (Peer peer : bucketPeer.getReplicas()) {
                serverBuckets.computeIfAbsent(peer.getHost(), k -> new HashSet<>())
                        .add(bucketPeer.getBucketId());
            }
        }

        // Remove a server
        Peer serverToRemove = servers.get(2); // Server-C
        Map<Integer, DataMovementPlan> movementPlans = hashRing.removeServer(serverToRemove);

        // Verify movement plans
        assertTrue(!movementPlans.isEmpty(), "Should have movement plans after server removal");

        // Get final state for verification
        Map<Integer, List<Peer>> finalBucketAssignments = hashRing.getBucketPeers().stream()
                .collect(Collectors.toMap(
                        BucketPeer::getBucketId,
                        BucketPeer::getReplicas
                ));

        // Verify each movement plan
        for (Map.Entry<Integer, DataMovementPlan> entry : movementPlans.entrySet()) {
            int bucketId = entry.getKey();
            DataMovementPlan plan = entry.getValue();

            // Verify removed server isn't in existing holders
            assertFalse(plan.getExistingHolders().stream()
                            .map(Peer::getHost)
                            .collect(Collectors.toSet())
                            .contains(serverToRemove.getHost()),
                    "Removed server should not be in existing holders");

            // Verify new recipients don't include existing holders
            Set<String> existingHolderHosts = plan.getExistingHolders().stream()
                    .map(Peer::getHost)
                    .collect(Collectors.toSet());

            for (Peer newRecipient : plan.getNewRecipients()) {
                assertFalse(existingHolderHosts.contains(newRecipient.getHost()),
                        "New recipient should not be in existing holders: " + newRecipient.getHost());
            }

            // Verify all current peers are accounted for
            List<Peer> currentPeers = finalBucketAssignments.get(bucketId);
            Set<String> currentHosts = currentPeers.stream()
                    .map(Peer::getHost)
                    .collect(Collectors.toSet());

            Set<String> allPlannedHosts = new HashSet<>();
            allPlannedHosts.addAll(plan.getExistingHolders().stream()
                    .map(Peer::getHost)
                    .collect(Collectors.toSet()));
            allPlannedHosts.addAll(plan.getNewRecipients().stream()
                    .map(Peer::getHost)
                    .collect(Collectors.toSet()));

            assertEquals(currentHosts, allPlannedHosts,
                    "All current peers should be accounted for in the movement plan");
        }
    }

    @Test
    void testReassignmentDeltaAccuracy() {
        // Add initial servers
        servers.forEach(hashRing::addServer);

        // Get initial assignments - track which servers have which bucket data
        Map<String, Set<Integer>> serverBuckets = new HashMap<>();
        for (BucketPeer bucketPeer : hashRing.getBucketPeers()) {
            for (Peer peer : bucketPeer.getReplicas()) {
                serverBuckets.computeIfAbsent(peer.getHost(), k -> new HashSet<>())
                        .add(bucketPeer.getBucketId());
            }
        }

        // Remove Server-C and record movement plans
        Peer serverToRemove = servers.get(2); // Server-C
        Map<Integer, DataMovementPlan> movementPlans = hashRing.removeServer(serverToRemove);

        // Verify at least some movement plans show partial data movement
        boolean foundPartialMovement = false;
        for (DataMovementPlan plan : movementPlans.values()) {
            if (!plan.getExistingHolders().isEmpty() && !plan.getNewRecipients().isEmpty()) {
                foundPartialMovement = true;
                break;
            }
        }
        assertTrue(foundPartialMovement,
                "Should find at least one case where some servers keep data and others need it");

        // Add a new server to see how it affects data movement
        Peer serverE = new Peer("Server-E", false);
        hashRing.addServer(serverE);

        // Verify replication factor is maintained
        for (BucketPeer bp : hashRing.getBucketPeers()) {
            assertEquals(REPLICATION_FACTOR, bp.getReplicas().size(),
                    "Each bucket should maintain replication factor after all changes");
        }
    }

    @Test
    void testReplicationFactor() {
        servers.forEach(hashRing::addServer);

        // Test multiple random keys
        Random random = new Random(42);
        for (int i = 0; i < 100; i++) {
            long keyHash = random.nextLong();
            BucketReplicasEntry replicas = hashRing.getReplicasForKey(keyHash);

            assertEquals(
                    REPLICATION_FACTOR,
                    replicas.getPeers().size(),
                    "Each key should have exactly " + REPLICATION_FACTOR + " replicas"
            );

            // Verify replicas are unique
            Set<String> uniqueHosts = new HashSet<>();
            replicas.getPeers().forEach(peer -> uniqueHosts.add(peer.getHost()));
            assertEquals(
                    REPLICATION_FACTOR,
                    uniqueHosts.size(),
                    "All replicas should be on different hosts"
            );
        }
    }

    @Test
    void testBucketPeerConsistency() {
        servers.forEach(hashRing::addServer);

        List<BucketPeer> bucketPeers = hashRing.getBucketPeers();

        assertEquals(
                TOTAL_BUCKETS,
                bucketPeers.size(),
                "Should have entry for each bucket"
        );

        // Verify each bucket has correct replication
        for (BucketPeer bucketPeer : bucketPeers) {
            assertEquals(
                    REPLICATION_FACTOR,
                    bucketPeer.getReplicas().size(),
                    "Each bucket should have correct number of replicas"
            );

            // Verify bucket number is within range
            assertTrue(
                    bucketPeer.getBucketId() >= 0 && bucketPeer.getBucketId() < TOTAL_BUCKETS,
                    "Bucket number should be within valid range"
            );
        }
    }

    @Test
    void testReassignmentWithVirtualNodes() {
        // Add initial servers
        servers.forEach(hashRing::addServer);

        // Get initial assignments - track which servers have which bucket data
        Map<String, Set<Integer>> serverBuckets = new HashMap<>();
        for (BucketPeer bucketPeer : hashRing.getBucketPeers()) {
            for (Peer peer : bucketPeer.getReplicas()) {
                serverBuckets.computeIfAbsent(peer.getHost(), k -> new HashSet<>())
                        .add(bucketPeer.getBucketId());
            }
        }

        // Remove Server-C
        Peer serverToRemove = servers.get(2); // Server-C
        Map<Integer, DataMovementPlan> reassignments = hashRing.removeServer(serverToRemove);

        // For each reassignment, verify that if a server already had the bucket data
        // (through any virtual node), it's not marked as needing new data
        for (DataMovementPlan reassignment : reassignments.values()) {
            int bucketId = reassignment.getBucketId();

            // Check each "new" replica
            for (Peer newReplica : reassignment.getNewRecipients()) {
                Set<Integer> existingBuckets = serverBuckets.get(newReplica.getHost());
                assertFalse(
                        existingBuckets != null && existingBuckets.contains(bucketId),
                        String.format(
                                "Server %s was marked as needing data for bucket %d but already had it through a virtual node",
                                newReplica.getHost(),
                                bucketId
                        )
                );
            }

            // Verify servers that already had the data aren't in newReplicas
            for (Peer currentReplica : reassignment.getExistingHolders()) {
                Set<Integer> existingBuckets = serverBuckets.get(currentReplica.getHost());
                if (existingBuckets != null && existingBuckets.contains(bucketId)) {
                    assertFalse(
                            reassignment.getNewRecipients().contains(currentReplica),
                            String.format(
                                    "Server %s shouldn't be in newRecipients for bucket %d as it already had the data",
                                    currentReplica.getHost(),
                                    bucketId
                            )
                    );
                }
            }
        }

        // Additional test with high number of virtual nodes to increase chance of overlap
        ConsistentHashRing highVirtualNodeRing = new ConsistentHashRing(
                32,  // Smaller number of buckets to increase chances of virtual node overlap
                256, // Much higher number of virtual nodes
                2,   // Same replication factor
                LongHashFunction.xx()
        );

        // Add servers and track initial assignments
        servers.forEach(highVirtualNodeRing::addServer);
        Map<String, Set<Integer>> highVirtualNodeServerBuckets = new HashMap<>();
        for (BucketPeer bucketPeer : highVirtualNodeRing.getBucketPeers()) {
            for (Peer peer : bucketPeer.getReplicas()) {
                highVirtualNodeServerBuckets.computeIfAbsent(peer.getHost(), k -> new HashSet<>())
                        .add(bucketPeer.getBucketId());
            }
        }

        // Remove a server and verify reassignments
        Map<Integer, DataMovementPlan> highVirtualNodeReassignments =
                highVirtualNodeRing.removeServer(serverToRemove);

        // Verify no server is marked as needing data for buckets it already has
        for (DataMovementPlan reassignment : highVirtualNodeReassignments.values()) {
            for (Peer newReplica : reassignment.getNewRecipients()) {
                Set<Integer> existingBuckets = highVirtualNodeServerBuckets.get(newReplica.getHost());
                assertFalse(
                        existingBuckets != null && existingBuckets.contains(reassignment.getBucketId()),
                        String.format(
                                "With high virtual nodes, server %s was marked as needing data for bucket %d but already had it",
                                newReplica.getHost(),
                                reassignment.getBucketId()
                        )
                );
            }
        }
    }

    @Test
    void testBucketIdStability() {
        // Add initial servers
        servers.forEach(hashRing::addServer);

        // Generate some test keys and get their initial bucket assignments
        Map<Long, Integer> keyToBucket = new HashMap<>();
        Random random = new Random(42);
        for (int i = 0; i < 100; i++) {
            long keyHash = random.nextLong();
            BucketReplicasEntry entry = hashRing.getReplicasForKey(keyHash);
            keyToBucket.put(keyHash, entry.getBucket());
        }

        // Remove a server
        Peer serverToRemove = servers.get(2); // Server-C
        hashRing.removeServer(serverToRemove);

        // Add a new server
        Peer newServer = new Peer("Server-E", false);
        hashRing.addServer(newServer);

        // Verify bucket assignments haven't changed
        for (Map.Entry<Long, Integer> entry : keyToBucket.entrySet()) {
            long keyHash = entry.getKey();
            int originalBucket = entry.getValue();

            BucketReplicasEntry newEntry = hashRing.getReplicasForKey(keyHash);
            assertEquals(originalBucket, newEntry.getBucket(),
                    "Bucket ID for key should not change after server changes");
        }
    }

    @Test
    void testVirtualNodeDataMovement() {
        // Add initial servers
        servers.forEach(hashRing::addServer);

        // Track initial bucket assignments
        Map<String, Set<Integer>> serverBuckets = new HashMap<>();
        for (BucketPeer bucketPeer : hashRing.getBucketPeers()) {
            for (Peer peer : bucketPeer.getReplicas()) {
                serverBuckets.computeIfAbsent(peer.getHost(), k -> new HashSet<>())
                        .add(bucketPeer.getBucketId());
            }
        }

        // Remove Server-C
        Peer serverToRemove = servers.get(2);
        Map<Integer, DataMovementPlan> movementPlans = hashRing.removeServer(serverToRemove);

        // Verify for each movement plan
        for (DataMovementPlan plan : movementPlans.values()) {
            int bucketId = plan.getBucketId();

            // Verify servers that already had the bucket aren't in new recipients
            for (Peer newRecipient : plan.getNewRecipients()) {
                Set<Integer> existingBuckets = serverBuckets.get(newRecipient.getHost());
                assertFalse(
                        existingBuckets != null && existingBuckets.contains(bucketId),
                        String.format(
                                "Server %s shouldn't need data for bucket %d as it already had it through a virtual node",
                                newRecipient.getHost(),
                                bucketId
                        )
                );
            }

            // Verify servers that had the data are in existing holders
            for (Peer existingHolder : plan.getExistingHolders()) {
                Set<Integer> existingBuckets = serverBuckets.get(existingHolder.getHost());
                assertTrue(
                        existingBuckets != null && existingBuckets.contains(bucketId),
                        String.format(
                                "Server %s should be an existing holder for bucket %d as it had the data",
                                existingHolder.getHost(),
                                bucketId
                        )
                );
            }
        }

        // Test with high virtual node count for better coverage
        ConsistentHashRing highVirtualNodeRing = new ConsistentHashRing(
                32,  // Fewer buckets
                256, // More virtual nodes
                2,   // Same replication factor
                HashConstants.HASH_FUNCTION
        );

        servers.forEach(highVirtualNodeRing::addServer);
        Map<Integer, DataMovementPlan> highVirtualNodePlans =
                highVirtualNodeRing.removeServer(serverToRemove);

        // Verify the same properties with high virtual node count
        for (DataMovementPlan plan : highVirtualNodePlans.values()) {
            assertFalse(
                    plan.getNewRecipients().stream()
                            .anyMatch(peer -> plan.getExistingHolders().contains(peer)),
                    "With high virtual nodes, no server should be both an existing holder and new recipient"
            );
        }
    }

}
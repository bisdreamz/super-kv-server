package org.reset.replication.merkle;

import com.nimbus.routing.HashConstants;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SimpleMerkleTest {

    @Test
    public void testMerkleConsistency() {
        int numBuckets = 512; // Smaller number for easier debugging
        LongHashFunction hashFunction = HashConstants.HASH_FUNCTION;

        // Create two servers
        SimpleMerkle serverA = new SimpleMerkle(numBuckets, hashFunction);
        SimpleMerkle serverB = new SimpleMerkle(numBuckets, hashFunction);

        // Track live data: key -> (valueHash, bucketIdx)
        Map<Long, LiveData> liveData = new HashMap<>();

        Random random = new Random(42); // Fixed seed for reproducibility
        int operations = 100000;

        // Perform random operations on serverA
        for (int i = 0; i < operations; i++) {
            int operation = random.nextInt(3); // 0=insert, 1=update, 2=delete

            switch (operation) {
                case 0: // Insert new
                    long keyHash = hashFunction.hashLong(random.nextLong());
                    long valueHash = random.nextLong();
                    int bucketIdx = Math.abs((int)(keyHash % numBuckets));

                    serverA.set(keyHash, valueHash, bucketIdx);
                    liveData.put(keyHash, new LiveData(valueHash, bucketIdx));

                    break;

                case 1: // Update existing
                    if (!liveData.isEmpty()) {
                        keyHash = getRandomKey(liveData, random);
                        LiveData existing = liveData.get(keyHash);
                        valueHash = random.nextLong(); // New value

                        serverA.set(keyHash, valueHash, existing.bucketIdx);
                        liveData.put(keyHash, new LiveData(valueHash, existing.bucketIdx));
                    }
                    break;

                case 2: // Delete existing
                    if (!liveData.isEmpty()) {
                        keyHash = getRandomKey(liveData, random);
                        bucketIdx = liveData.get(keyHash).bucketIdx;

                        serverA.delete(keyHash, bucketIdx);
                        liveData.remove(keyHash);
                    }
                    break;
            }
        }

        // Now populate serverB with the final state
        for (Map.Entry<Long, LiveData> entry : liveData.entrySet()) {
            long keyHash = entry.getKey();
            LiveData data = entry.getValue();
            serverB.set(keyHash, data.valueHash, data.bucketIdx);
        }

        // Compare all buckets
        System.out.println("\nComparing bucket hashes:");
        for (int i = 0; i < numBuckets; i++) {
            long hashA = serverA.getBucketHash(i);
            long hashB = serverB.getBucketHash(i);

            assertEquals(hashA, hashB,
                    String.format("Bucket %d hashes don't match: ServerA=%d, ServerB=%d",
                            i, hashA, hashB));
        }
    }

    private static long getRandomKey(Map<Long, LiveData> map, Random random) {
        Object[] keys = map.keySet().toArray();
        return (Long) keys[random.nextInt(keys.length)];
    }

    private static class LiveData {
        final long valueHash;
        final int bucketIdx;

        LiveData(long valueHash, int bucketIdx) {
            this.valueHash = valueHash;
            this.bucketIdx = bucketIdx;
        }
    }
}
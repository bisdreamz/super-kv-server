package org.reset;

import java.time.Duration;

public class SuperConfig {

    private static final int HASHRING_TOTAL_BUCKETS = 1024;
    private static final int HASHRING_VIRTUALNODES_PER_SERVER = 32;

    public static enum ExpireAfter {
        NONE,
        AFTER_WRITE,
        AFTER_ACCESS,
    }

    private final String datastore;
    private final Duration expiryTtl;
    private final ExpireAfter expiryMethod;
    private final int replicas;

    public SuperConfig() {
        String datastore = System.getenv("DATASTORE");
        this.datastore = datastore != null && !datastore.isEmpty()
                ? datastore : null;

        String expiry = System.getenv("EXPIRY_TTL");
        try {
            this.expiryTtl = expiry != null && !expiry.isEmpty()
                    ? Duration.ofSeconds(Integer.parseInt(expiry)) : Duration.ZERO;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid EXPIRY value: " + expiry);
        }

        String expireAfter = System.getenv("EXPIRY_METHOD");
        if (expireAfter  == null || expireAfter.isEmpty()) {
            this.expiryMethod = ExpireAfter.NONE;
        } else {
            try {
                this.expiryMethod = ExpireAfter.valueOf(expireAfter.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid EXPIRY_METHOD value: " + expireAfter);
            }
        }

        String replicasString = System.getenv("REPLICAS");
        this.replicas = replicasString == null || replicasString.isEmpty()
                ? 1 : Integer.parseInt(replicasString);
        if (this.replicas < 1 || this.replicas > 100)
            throw new IllegalArgumentException("Invalid REPLICAS value: " + replicas);
    }

    public String getDatastore() {
        return datastore;
    }

    public Duration getExpiryTtl() {
        return expiryTtl;
    }

    public ExpireAfter getExpiryMethod() {
        return expiryMethod;
    }

    /**
     * @return Number of replicas to keep of data.
     * @implNote Default value is 1 if not specified
     * E.g. 1 means only one server holds the key, 2 means keep an extra copy, etc
     */
    public int getReplicas() {
        return replicas;
    }

    // TODO make hashring configurable
    public int getHashringTotalBuckets() {
        return HASHRING_TOTAL_BUCKETS;
    }

    public int getHashringVirtualNodesPerServer() {
        return HASHRING_VIRTUALNODES_PER_SERVER;
    }
}

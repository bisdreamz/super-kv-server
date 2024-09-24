package org.reset;

import java.time.Duration;

public class SuperConfig {

    public static enum ExpireAfter {
        NONE,
        AFTER_WRITE,
        AFTER_ACCESS,
    }

    private final String datastore;
    private final Duration expiryTtl;
    private final ExpireAfter expiryMethod;

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
}

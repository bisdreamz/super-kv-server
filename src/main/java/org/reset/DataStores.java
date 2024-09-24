package org.reset;

import org.reset.datastore.DataStore;
import org.reset.datastore.HashMapDataStore;

import java.time.Duration;

public class DataStores {

    public static DataStore getDataStore(SuperConfig config) {
        String datastore = config.getDatastore();

        DataStore store = switch (datastore) {
            case "HashMap" -> new HashMapDataStore();
            case null -> new HashMapDataStore();
            default -> null;
        };

        if (store == null)
            throw new IllegalArgumentException("Datastore " + datastore + " not found");

        if (config.getExpiryTtl() != Duration.ZERO
                && config.getExpiryMethod() != SuperConfig.ExpireAfter.NONE) {
            if (config.getExpiryMethod() == SuperConfig.ExpireAfter.AFTER_WRITE
                && !store.supportsExpiryAfterWrite()) {
                throw new IllegalArgumentException("DataStore selected does not support ExpireAfterWrite");
            } else if (config.getExpiryMethod() == SuperConfig.ExpireAfter.AFTER_ACCESS
                && !store.supportsExpiryAfterRead()) {
                throw new IllegalArgumentException("DataStore selected does not support ExpireAfterRead");
            }
        }

        return store;
    }

}

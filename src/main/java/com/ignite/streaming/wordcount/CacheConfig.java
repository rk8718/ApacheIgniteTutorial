package com.ignite.streaming.wordcount;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.configuration.CacheConfiguration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CacheConfig {

    public static CacheConfiguration<AffinityUuid, String> wordCache() {
        CacheConfiguration<AffinityUuid, String> cfg = new CacheConfiguration<>("words");

        // Index all words streamed into cache.
        cfg.setIndexedTypes(AffinityUuid.class, String.class);

        // Sliding window of 1 seconds.
        cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, 2))));

        return cfg;
    }
}
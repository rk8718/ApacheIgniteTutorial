package com.ignite.sql;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.Arrays;
import java.util.Collection;

public class IgniteSqlExample {

    public static void main(String[] args) {
        Ignition.setClientMode(true);

        Ignite ignite = Ignition.start("config/example-ignite.xml");

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");
        cache.put(1, "Hello");
        cache.put(2, "World!");

        // Get values from cache
        // Broadcast 'Hello World' on all the nodes in the cluster.
        ignite.compute().broadcast(()->System.out.println(cache.get(1) + " " + cache.get(2)));


        Collection<Integer> res = ignite.compute().apply(
                (String word) -> {
                    System.out.println("Word: " + word);
                    return word.length();
                },
                Arrays.asList("Hello World India".split(" "))
        );

        int total = res.stream().mapToInt(Integer::intValue).sum();
        System.out.println("total : " + total);
    }
}

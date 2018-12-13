package com.ignite.streaming.wordcount;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import com.ignite.ExamplesUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityUuid;

public class StreamWords {

    public static void main(String[] args) throws Exception {

        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            IgniteCache<AffinityUuid, String> stmCache = ignite.getOrCreateCache(CacheConfig.wordCache());

            try (IgniteDataStreamer<AffinityUuid, String> stmr = ignite.dataStreamer(stmCache.getName())) {

                while (true) {
                    InputStream in = new FileInputStream("config/alice-in-wonderland.txt");

                    try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in))) {
                        for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                            for (String word : line.split(" "))
                                if (!word.isEmpty()) {
                                    stmr.addData(new AffinityUuid(word), word);
                                }
                        }
                    }
                }
            }
        }
    }
}
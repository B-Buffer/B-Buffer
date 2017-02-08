package org.bbuffer.teiid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.openjdk.jmh.annotations.Benchmark;

public class SkipListMapBencharmk {

    static final int nCycles = 50000;
    static final int nRep = 10;
    static final int dataSize = nCycles / 4;
    static final List<String> data = new ArrayList<>(nCycles);
    static final Map<String,String> hmap4get = new ConcurrentHashMap<>(3000, 0.5f, 10);
    static final Map<String,String> smap4get = new ConcurrentSkipListMap<>();
    
    static {
        // prepare data
        List<String> values = new ArrayList<>(dataSize);
        for( int i = 0; i < dataSize; i++ ) {
            values.add(UUID.randomUUID().toString());
        }
        // rehash data for all cycles
        for( int i = 0; i < nCycles; i++ ) {
            data.add(values.get((int)(Math.random() * dataSize)));
        }
        // rehash data for all cycles
        for( int i = 0; i < dataSize; i++ ) {
            String value = data.get((int)(Math.random() * dataSize));
            hmap4get.put(value, value);
            smap4get.put(value, value);
        }
    }
    
    @Benchmark
    public void skipList_put() {
        for( int n = 0; n < nRep; n++ ) {
            Map<String,String> map = new ConcurrentSkipListMap<>();

            for( int i = 0; i < nCycles; i++ ) {
                String key = data.get(i);
                map.put(key, key);
            }
        }
    }
    
    @Benchmark
    public void skipListMap_get() {
        for( int n = 0; n < nRep; n++ ) {
            for( int i = 0; i < nCycles; i++ ) {
                String key = data.get(i);
                smap4get.get(key);
            }
        }
    }
    
    @Benchmark
    public void hashMap_put() {
        for( int n = 0; n < nRep; n++ ) {
            Map<String,String> map = new ConcurrentHashMap<>(3000, 0.5f, 10);

            for( int i = 0; i < nCycles; i++ ) {
                String key = data.get(i);
                map.put(key, key);
            }
        }
    }
    
    @Benchmark
    public void hasMap_get() {
        for( int n = 0; n < nRep; n++ ) {
            for( int i = 0; i < nCycles; i++ ) {
                String key = data.get(i);
                hmap4get.get(key);
            }
        }
    }
}

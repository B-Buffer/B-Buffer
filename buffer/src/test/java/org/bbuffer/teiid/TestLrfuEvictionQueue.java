package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.impl.LrfuEvictionQueue;

public class TestLrfuEvictionQueue {

    @Test
    public void testInitialize() {
        
        LrfuEvictionQueue<?> queue = new LrfuEvictionQueue<CacheEntry>(new AtomicLong());
        assertEquals(0, queue.getEvictionQueue().size());
        assertEquals(0, queue.getSize());
    }
    
    @Test
    public void testAddTouchRemove() {
        LrfuEvictionQueue<CacheEntry> queue = new LrfuEvictionQueue<CacheEntry>(new AtomicLong(1024));
        CacheEntry e1 = new CacheEntry(1L);
        queue.add(e1);
        assertEquals(1, queue.getSize());
        queue.touch(e1);
        assertEquals(1, queue.getSize());
        queue.remove(e1);
        assertEquals(0, queue.getSize());
    }

    @Test
    public void testSkipListMapBencharmk() throws Exception {
        // java -jar target/benchmarks.jar -bm avgt -f 1 -wi 5 -i 5 -t 1
        Options opt = new OptionsBuilder()
                .include(SkipListMapBencharmk.class.getSimpleName())
                .mode(Mode.AverageTime)
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(3)
                .threads(1)
                .build();
        
        new Runner(opt).run();
    }
}

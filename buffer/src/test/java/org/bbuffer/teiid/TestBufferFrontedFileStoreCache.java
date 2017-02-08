package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.logging.Level;

import org.bbuffer.TestHelper;
import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.util.FileUtils;

public class TestBufferFrontedFileStoreCache {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }
    
    String storageDir = "target" + File.separator + "buffer";
    
    final static class SimpleSerializer implements Serializer<Integer>  {

        @Override
        public void serialize(Integer obj, ObjectOutput oos) throws IOException {
            oos.writeInt(obj);
            for(int i = 0 ; i < obj ; i ++)
                oos.writeInt(i);
        }

        @Override
        public Integer deserialize(ObjectInput ois) throws IOException, ClassNotFoundException {
            Integer result = ois.readInt();
            for (int i = 0; i < result; i++)
                assertEquals(i, ois.readInt());
            return result;
        }

        @Override
        public boolean useSoftCache() {
            return false;
        }

        @Override
        public Long getId() {
            return 1l;
        }
        
    }
    
    @Before
    public void clean() {
        Path path = Paths.get(storageDir);
        if(!Files.exists(path)) {
            return;
        }
        FileUtils.removeChildrenRecursively(new File(storageDir));
    }
    
    @Test
    public void testInitialize_01() {
        long memoryBufferSpace = 1 << 26; //64MB
        assertEquals(1 << 13, memoryBufferSpace >> 13);
        int addressPerBlock = (1 << 13) / (1 << 2) ;
        assertEquals(1 << 11, addressPerBlock);
        int blocks = (int) ((memoryBufferSpace >> 13) * addressPerBlock / (addressPerBlock + 1));
        assertEquals(8188, blocks);
    }
    
    @Test
    public void testInitialize_02() {
        
        int a = 0b11111;
        int b = 0b10101;
        int c = 0b100000; 
        int ac = 0b111111;
        int bc = 0b110101;
        
        assertEquals(b, a & b);
        assertEquals(0, a & c);
        assertEquals(0, b & c);
        assertEquals(a, a | b);
        assertEquals(ac, a | c);
        assertEquals(bc, b | c);
        
        assertEquals(10, a % b);
        assertEquals(0, (1 << 30) % (1 << 15));
        assertEquals(0, (1 << 30) % (1 << 10));
        assertEquals(1, ((1 << 30) + 1) % (1 << 15));
        assertEquals(1, ((1 << 30) + 1) % (1 << 10));
    }
    
    @Test
    public void testInitialize_03() {
        //sets a bit to true (or 1). 
        BitSet b = new BitSet();
        b.set(2);
        b.set(10);
        assertTrue(b.get(2));
        assertTrue(b.get(10));
        assertEquals(11, b.length());
        assertFalse(b.get(11));
        b.set(3, 5);
        assertTrue(b.get(3));
        assertTrue(b.get(4));
        assertFalse(b.get(5));
        
        
        // flip - changes the value of a 0 bit to 1, and the value of a 1 bit to zero
        b.set(11);
        assertEquals(12, b.length());
        assertTrue(b.get(11));
        b.flip(11);
        assertFalse(b.get(11));
        assertFalse(b.get(8));
        b.flip(8);
        assertTrue(b.get(8));
        
        //ToByteArray
        b = new BitSet();
        b.set(0, 7);
        assertEquals(1, b.toByteArray().length);
        assertEquals(127, b.toByteArray()[0]);
        b.set(8, 15);
        assertEquals(2, b.toByteArray().length);
        byte[] bytes = b.toByteArray();
        assertEquals(127, bytes[0]);
        assertEquals(127, bytes[1]);
        
        //clear
        b.clear(0, 2);
        assertFalse(b.get(0));
        assertFalse(b.get(1));
        assertTrue(b.get(2));
        assertTrue(b.get(3));
        
    }


    @Test
    public void testInitialize() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        SplittableStorageManager ssm = new SplittableStorageManager(fsm);
        ssm.setMaxFileSize(10);
        
        BufferFrontedFileStoreCache cache = new BufferFrontedFileStoreCache();
        cache.setStorageManager(ssm);
        cache.initialize();
        
        CacheEntry ce = new CacheEntry(2l);
        Serializer<Integer> s = new SimpleSerializer();
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(2);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);
        assertEquals(cacheObject, ce.getObject());
    }

}

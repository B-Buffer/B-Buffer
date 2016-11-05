package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.logging.Level;

import org.bbuffer.TestHelper;
import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.OutOfDiskException;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager.SplittableFileStore;
import org.teiid.core.util.FileUtils;

public class TestSplittableStorageManager {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }
    
    static Random r = new Random();
    
    String storageDir = "target" + File.separator + "buffer";
    
    @Before
    public void clean() {
        Path path = Paths.get(storageDir);
        if(!Files.exists(path)) {
            return;
        }
        FileUtils.removeChildrenRecursively(new File(storageDir));
    }
    
    @Test
    public void testWriteRead() throws Exception {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        SplittableStorageManager ssm = new SplittableStorageManager(fsm);
        ssm.setMaxFileSize(10);
        ssm.initialize();
        
        String tsID = "0"; 
        SplittableFileStore store = (SplittableFileStore) ssm.createFileStore(tsID);
        byte[] bytes = new byte[10000];
        r.nextBytes(bytes);
        store.write(0, bytes, 0, bytes.length);
        
        byte[] bytesRead = new byte[10000];        
        store.readFully(0, bytesRead, 0, bytesRead.length);
        
        assertArrayEquals(bytes, bytesRead);
        assertEquals(1, fsm.getOpenFiles());
        assertEquals(10000, fsm.getUsedBufferSpace());
        
        store.remove();
        assertEquals(0, fsm.getOpenFiles());
        assertEquals(0, fsm.getUsedBufferSpace());
    }

    @Test
    public void testCreatesSpillFiles() throws Exception {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        SplittableStorageManager ssm = new SplittableStorageManager(fsm);
        ssm.setMaxFileSizeDirect(2048);
        ssm.initialize();
        
        String tsID = "0"; 
        SplittableFileStore store = (SplittableFileStore) ssm.createFileStore(tsID);
        byte[] bytes = new byte[4096];
        r.nextBytes(bytes);
        store.write(0, bytes, 0, bytes.length);
        assertEquals(2, fsm.getOpenFiles());
        assertEquals(4096, fsm.getUsedBufferSpace());
        
        store.setLength(10000);
        assertEquals(5, fsm.getOpenFiles());
        assertEquals(10000, fsm.getUsedBufferSpace());
        
        store.setLength(100);
        assertEquals(1, fsm.getOpenFiles());
        assertEquals(100, fsm.getUsedBufferSpace());
        
        store.removeDirect();
        assertEquals(0, fsm.getOpenFiles());
        assertEquals(0, fsm.getUsedBufferSpace());
    }
    
    @Test(expected = OutOfDiskException.class)
    public void testMaxSpaceSplitFailed() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1);
        SplittableStorageManager ssm = new SplittableStorageManager(fsm);
        ssm.setMaxFileSizeDirect(2048);
        ssm.initialize();
        
        String tsID = "0"; 
        SplittableFileStore store = (SplittableFileStore) ssm.createFileStore(tsID);
        byte[] bytes = new byte[4096];
        r.nextBytes(bytes);
        try {
            store.write(0, bytes, 0, bytes.length);
        } finally {
            assertEquals(1, fsm.getOpenFiles());
            assertEquals(0, fsm.getUsedBufferSpace());
        }
    }
}

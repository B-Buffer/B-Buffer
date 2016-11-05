package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;

import org.bbuffer.TestHelper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.OutOfDiskException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.FileUtils;

public class TestFileStorageManager {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }
    
    String storageDir = "target" + File.separator + "buffer";
    
    @Before
    public void clean() {
        Path path = Paths.get(storageDir);
        if(!Files.exists(path)) {
            return;
        }
        FileUtils.removeChildrenRecursively(new File(storageDir));
    }
    
    static Random r = new Random();
    
    static void writeBytes(FileStore store) throws IOException {
        writeBytes(store, store.getLength());
    }
    
    static byte[] writeBytes(FileStore store, long start) throws IOException {
        byte[] bytes = new byte[2048];
        r.nextBytes(bytes);
        store.write(start, bytes, 0, bytes.length);
        byte[] bytesRead = new byte[2048]; 
        store.readFully(start, bytesRead, 0, bytesRead.length);
        assertTrue(Arrays.equals(bytes, bytesRead));
        return bytes;
    }
    
    @Test
    public void testInitialize() throws TeiidComponentException {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        assertEquals(storageDir, fsm.getDirectory());
        assertEquals(0, fsm.getOpenFiles());
        assertEquals(0, fsm.getUsedBufferSpace());
        assertEquals(256, new File(storageDir).listFiles().length);
    }
    
    @Test
    public void testInitialRead() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        assertEquals(0, store.getLength());
        int length = store.read(0, new byte[1], 0, 1);
        assertEquals( -1, length);
    }
    
    @Test
    public void testWrite() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        int kb = 1 << 10;
        byte[] bytes = new byte[kb];
        r.nextBytes(bytes);
        store.write(0, bytes, 0, bytes.length);
        
        assertEquals(1024, fsm.getUsedBufferSpace());
        assertEquals(1, fsm.getOpenFiles());
        
        byte[] bytesRead = new byte[1024]; 
        store.readFully(0, bytesRead, 0, bytesRead.length);
        assertTrue(Arrays.equals(bytes, bytesRead));
        
        bytesRead = new byte[16];
        long fileOffset = 1 << 7;
        store.read(fileOffset, bytesRead, 0, 16);
        for(int i = 0 ; i < 16 ; i ++) {
            assertEquals(bytesRead[i], bytes[(int) (i + fileOffset)]);
        }
        
        store.remove();
        assertEquals(0, fsm.getUsedBufferSpace());
        assertEquals(0, fsm.getOpenFiles());
        
    }
    
    @Test(expected = OutOfDiskException.class)
    public void testMaxSpace() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 10);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        int size = 1 << 10;
        byte[] bytes = new byte[size + 1];
        r.nextBytes(bytes);
        store.write(0, bytes, 0, bytes.length);
    }
    
    @Ignore
    @Test
    public void testMaxSpace_2() throws Exception {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 6);
        fsm.setMaxBufferSpace((1 << 20) * 10);
        fsm.initialize();
        
        int count = Runtime.getRuntime().availableProcessors() * 2 ;
        Thread[] array = new Thread[count];
        for(int i = 0 ; i < count ; i ++) {
            Thread t = new Thread(() -> {
                while(true){
                    FileStore store = fsm.createFileStore(UUID.randomUUID().toString());
                    int size = 1 << 20;
                    byte[] bytes = new byte[size];
                    r.nextBytes(bytes);
                    try {
                        store.write(0, bytes, 0, bytes.length);
                    } catch (Exception e) {
                        
                    }
                }
            });
            t.start();
            array[i] = t;
        }
        
        for(Thread t : array){
            t.join();
        }
    }
    
    @Test
    public void testPositionalWrite() throws Exception {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        byte[] expectedBytes = writeBytes(store, 2048);
        assertEquals(4096, fsm.getUsedBufferSpace());
        
        writeBytes(store, 4096);
        assertEquals(6144, fsm.getUsedBufferSpace());
        
        byte[] bytesRead = new byte[2048];        
        store.readFully(2048, bytesRead, 0, bytesRead.length);
        
        assertArrayEquals(expectedBytes, bytesRead);
        
        store.remove();
        assertEquals(0, fsm.getUsedBufferSpace());
    }
    
    @Test
    public void testSetLength() throws Exception {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        store.setLength(1 << 10);
        assertEquals(1 << 10, fsm.getUsedBufferSpace());
        
        store.setLength(1 << 9);
        assertEquals(1 << 9, fsm.getUsedBufferSpace());
    }
    
    @Test
    public void testFileStoreOutputStream() throws TeiidComponentException, IOException {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        FileStoreOutputStream fsos = store.createOutputStream(1 << 10);
        byte[] bytes = new byte[1 << 10];
        r.nextBytes(bytes);
        fsos.write(bytes);
        fsos.write(1);
        fsos.flush();
        assertEquals(1025, fsm.getUsedBufferSpace());
    }
    
    @Test
    public void testFileStoreOutputStream_2() throws TeiidComponentException, IOException {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        FileStoreOutputStream fsos = store.createOutputStream(1 << 10);
        byte[] bytes = new byte[1 << 5];
        r.nextBytes(bytes);
        fsos.write(bytes);
        fsos.write(1);
        fsos.write(1);
        fsos.write(1);
        assertEquals(0, fsm.getUsedBufferSpace());
        assertEquals(35, fsos.getCount());
        assertFalse(fsos.bytesWritten());
        byte[] bytesInFsosBuffer = new byte[1 << 5];
        System.arraycopy(fsos.getBuffer(), 0, bytesInFsosBuffer, 0 , 1 << 5);
        assertArrayEquals(bytes, bytesInFsosBuffer);
        assertEquals(1, fsos.getBuffer()[32]);
        assertEquals(1, fsos.getBuffer()[33]);
        assertEquals(1, fsos.getBuffer()[34]);
        fsos.flush();
        assertEquals(0, fsm.getUsedBufferSpace());
        fsos.flushBuffer();
        assertEquals(35, fsm.getUsedBufferSpace());
        
        byte[] expectedBytes = new byte[1 << 5];
        store.readFully(0, expectedBytes, 0, 32);
        assertArrayEquals(bytes, expectedBytes);
        assertArrayEquals(bytesInFsosBuffer, expectedBytes);
        
        store.remove();
        assertEquals(0, fsm.getUsedBufferSpace());
    }
    
    @Test
    public void testFileStoreOutputStream_3() throws TeiidComponentException, IOException {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        FileStoreOutputStream fsos = store.createOutputStream(1 << 15);
        
        assertEquals(1 << 8, fsos.getBuffer().length);
        
        byte[] bytes = new byte[1 << 14];
        r.nextBytes(bytes);
        fsos.write(1);
        fsos.write(bytes);
        fsos.flush();
        fsos.flushBuffer();
        assertEquals(1 << 15, fsos.getBuffer().length);       
        assertEquals((1 << 14) + 1, fsm.getUsedBufferSpace());
    }
    
    @Test
    public void testFileStoreOutputStream_4() throws TeiidComponentException, IOException {
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 20);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        FileStoreOutputStream fsos = store.createOutputStream(2);
        fsos.write(new byte[100000]);
        fsos.close();
        
        try {
            fsos.flush();
        } catch (IllegalStateException e) {
            assertEquals("Alread closed", e.getMessage());
        }
        
        assertEquals(null, fsos.getBuffer());
    }
    
    @Test
    public void testFileStoreOutputStream_5() throws TeiidComponentException, IOException {
        
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(storageDir);
        fsm.setMaxOpenFiles(1 << 3);
        fsm.setMaxBufferSpace(1 << 30);
        fsm.initialize();
        
        String tsID = "0";  
        FileStore store = fsm.createFileStore(tsID);
        FileStoreOutputStream fsos = store.createOutputStream(1 << 10);
        assertEquals(0, fsos.getCount());
        fsos.write(2);
        fsos.write(2);
        fsos.write(new byte[1 << 3]);
        assertEquals(10, fsos.getCount());
        fsos.flush();
        assertEquals(10, fsos.getCount());
        fsos.flushBuffer();
        assertEquals(0, fsos.getCount());
        fsos.write(new byte[1 << 20]);
        assertEquals(0, fsos.getCount());
        fsos.flush();
        assertEquals(0, fsos.getCount());
    }
}

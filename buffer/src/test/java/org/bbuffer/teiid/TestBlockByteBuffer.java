package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;
import org.teiid.common.buffer.impl.BlockByteBuffer;

public class TestBlockByteBuffer {
    
    static Random r = new Random();

    @Test
    public void testByteBuffer() {
        
        ByteBuffer buffer = ByteBuffer.allocate(10);      
        assertEquals(10, buffer.capacity());
        assertEquals(0, buffer.position());
        
        buffer.put(0, (byte)0xFF);
        assertEquals(-1, buffer.get(0));
        
        buffer.position(5);
        buffer.put((byte)0xFF);
        assertEquals(-1, buffer.get(5));
        assertEquals(6, buffer.position());
        assertEquals(4, buffer.remaining());
        
        buffer.limit(7);
        assertEquals(1, buffer.remaining());
        buffer.put((byte)0xFF);
        assertEquals(0, buffer.remaining());
        Exception ex = null;
        try {
            buffer.put((byte)0xFF);
        } catch (BufferOverflowException e) {
            ex = e;
        }
        assertEquals(ex.getClass(), BufferOverflowException.class);
        
        buffer.rewind();
        assertEquals(0, buffer.position());
        
        buffer.limit(10);
        byte[] bytes = new byte[10];
        r.nextBytes(bytes);
        buffer.put(bytes);
        assertArrayEquals(bytes, buffer.array());
        assertEquals(10, buffer.position());
        
        buffer.rewind();
        buffer.put((byte)0xFF);
        buffer.flip();
        assertEquals(1, buffer.remaining());
        assertEquals(0, buffer.position());
    }
    
    @Test
    public void testInitialize() {
        
        BlockByteBuffer bbb = new BlockByteBuffer(4, 2, 2, false);
        ByteBuffer buffer = bbb.getByteBuffer(1);
        assertEquals(4, buffer.position());
        assertEquals(1, bbb.getBuffers().length);
    }
    
    @Test
    public void testInitialize_1() {
        BlockByteBuffer bbb = new BlockByteBuffer(30, 1000000, 13, false);
        assertEquals(8, bbb.getBuffers().length);
    }
    
    public void testInitialize_2() {
        BlockByteBuffer bbb = new BlockByteBuffer(30, 8188, 13, false);
    }
}

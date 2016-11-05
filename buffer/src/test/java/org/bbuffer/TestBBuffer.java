package org.bbuffer;

import java.util.logging.Level;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;

public class TestBBuffer {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }

    @Test
    public void testBufferManagerInit() {
        
    }
    
    public static void main(String[] args) {
        BufferManager bm = BBuffer.Factory.getBuilder()
                .useDisk(true)
                .bufferDir("/home/kylin/tmp/buffer")
                .build();
    }
}

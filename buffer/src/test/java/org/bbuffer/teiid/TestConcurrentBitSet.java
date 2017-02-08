package org.bbuffer.teiid;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.common.buffer.impl.ConcurrentBitSet;

public class TestConcurrentBitSet {

    @Test
    public void testBitsSet() {
        ConcurrentBitSet cbs = new ConcurrentBitSet(50001, 4);
        assertEquals(0, cbs.getAndSetNextClearBit());
    }
}

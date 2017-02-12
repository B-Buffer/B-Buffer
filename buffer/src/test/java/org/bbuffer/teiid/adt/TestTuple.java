package org.bbuffer.teiid.adt;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestTuple {
 
    @Test
    public void testTupleLength() {
        List<?> tuple = Arrays.asList(10, "tuple", null);
        assertEquals(3, tuple.size());
    }
    
    @Test
    public void testTupleElement() {
        List<?> tuple = Arrays.asList(10, "tuple", null);
        assertEquals(10, tuple.get(0));
        assertEquals("tuple", tuple.get(1));
        assertEquals(null, tuple.get(2));
    }
    
    @Test
    public void testTupleElementType() {
        List<?> tuple = Arrays.asList(10, "tuple", null);
        assertEquals(Integer.class, tuple.get(0).getClass());
        assertEquals(String.class, tuple.get(1).getClass());
    }
}

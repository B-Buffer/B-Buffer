package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTuple {
 
    @Test
    public void testTupleLength() {
        List tuple = new ArrayList();
        tuple.add(new Integer(100));
        tuple.add(new String("tuple"));
        tuple.add(null);
        assertEquals(3, tuple.size());
    }
    
    @Test
    public void testTupleElement() {
        List tuple = new ArrayList();
        tuple.add(new Integer(100));
        tuple.add(new String("tuple"));
        tuple.add(null);
        assertEquals(new Integer(100), tuple.get(0));
        assertEquals("tuple", tuple.get(1));
        assertEquals(null, tuple.get(2));
    }
    
    @Test
    public void testTupleElementType() {
        List tuple = new ArrayList();
        tuple.add(new Integer(100));
        tuple.add(new String("tuple"));
        tuple.add(null);
        assertEquals(Integer.class, tuple.get(0).getClass());
        assertEquals(String.class, tuple.get(1).getClass());
    }
}

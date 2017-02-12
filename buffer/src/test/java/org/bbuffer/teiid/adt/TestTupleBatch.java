package org.bbuffer.teiid.adt;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.TupleBatch;

public class TestTupleBatch {

    public static TupleBatch example() {
        List<List<?>> rows = new ArrayList<>();
        rows.add(Arrays.asList("e0", "e1", "e2"));
        rows.add(Arrays.asList("e0", "e1", "e2"));
        rows.add(Arrays.asList("e0", "e1", "e2"));
        return new TupleBatch(1, rows);
    }
    
    @Test
    public void testBeginRowEndRow() {
        TupleBatch tuples = example();
        assertEquals(1, tuples.getBeginRow());
        assertEquals(3, tuples.getEndRow());
    }
    
    @Test
    public void testRowCount() {
        TupleBatch tuples = example();
        assertEquals(3, tuples.getRowCount());
    }
    
    @Test
    public void testIndex() {
        TupleBatch tuples = example();
        assertEquals("e2", tuples.getTuple(2).get(2));
        
        try {
            tuples.getTuple(3);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
        }
        
        assertEquals("e2", tuples.getTuples().get(2).get(2));
        assertEquals("e2", tuples.getAllTuples()[2].get(2));
    }
    
    @Test
    public void testOffset() {
        TupleBatch tuples = example();
        tuples.setRowOffset(2);
        assertFalse(tuples.containsRow(1));
    }
    
    @Test
    public void testTermination() {
        TupleBatch tuples = example();
        assertFalse(tuples.getTerminationFlag());
        assertEquals(0, tuples.getTermination());
        tuples.setTerminationFlag(true);
        assertTrue(tuples.getTerminationFlag());
        assertEquals(1, tuples.getTermination());
    }
    
    @Test
    public void testTupleBatch() {
        List<List<?>> tuples = new ArrayList<>();
        tuples.add(Arrays.asList(101, "John", 38));
        tuples.add(Arrays.asList(102, "Mary", 30));
        TupleBatch batch = new TupleBatch(9, tuples);
        batch.setTerminationFlag(true);
        assertEquals("TupleBatch; beginning row=9, number of rows=2, lastBatch=1", batch.toString());
    }
}

package org.bbuffer.teiid;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.TupleBatch;

@SuppressWarnings({ "unchecked", "rawtypes" })  
public class TestTupleBatch {

    public static TupleBatch example() {
        List rows = new ArrayList(3);
        for(int i = 0 ;  i < 3 ; i ++) {
            List row = new ArrayList(3);
            for(int j = 1 ; j <= 3 ; j ++) {
                row.add("D" + i);
            }
            rows.add(row);
        }
        return new TupleBatch(0, rows);
    }
    
    @Test
    public void testBeginRowEndRow() {
        TupleBatch tuples = example();
        assertEquals(0, tuples.getBeginRow());
        assertEquals(2, tuples.getEndRow());
    }
    
    @Test
    public void testRowCount() {
        TupleBatch tuples = example();
        assertEquals(3, tuples.getRowCount());
    }
    
    @Test
    public void testIndex() {
        TupleBatch tuples = example();
        assertEquals("D2", tuples.getTuple(2).get(2));
        
        try {
            tuples.getTuple(3);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
        }
        
        assertEquals("D2", tuples.getTuples().get(2).get(2));
        assertEquals("D2", tuples.getAllTuples()[2].get(2));
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
    public void testOffset() {
        TupleBatch tuples = example();
        tuples.setRowOffset(1);
        assertFalse(tuples.containsRow(0));
    }
    
    @Test
    public void testTupleBatch() {
        List rows = new ArrayList(3);
        List row1 = new ArrayList(3);
        List row2 = new ArrayList(3);
        row1.add(101);
        row1.add("John");
        row1.add(38);
        row2.add(102);
        row2.add("Kylin");
        row2.add(30);
        rows.add(row1);
        rows.add(row2);
        TupleBatch tuples = new TupleBatch(0, rows);
        assertEquals("TupleBatch; beginning row=0, number of rows=2, lastBatch=0", tuples.toString());
    }
}

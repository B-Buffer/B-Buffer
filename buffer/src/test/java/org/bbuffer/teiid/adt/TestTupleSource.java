package org.bbuffer.teiid.adt;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.CollectionTupleSource;

public class TestTupleSource {

    @Test
    public void testCollectionTupleSource() {
        List<List<?>> tuples = new ArrayList<>();
        for(int i = 0 ; i < 10 ; i ++) {
            tuples.add(Arrays.asList(i, "name-" + i));
        }
        CollectionTupleSource tupleSource = new CollectionTupleSource(tuples.iterator());
        List<?> tuple = null;
        Set<String> names = new HashSet<>();
        while((tuple = tupleSource.nextTuple()) != null) {
            names.add((String)tuple.get(1));
        }
        
        for(int i = 0 ; i < 10 ; i ++) {
            assertTrue(names.contains("name-" + i));
        }
    }
    
    @Test
    public void testUpdateCountArrayTupleSource() throws TeiidComponentException, TeiidProcessingException {
        TupleSource tupleSource = CollectionTupleSource.createUpdateCountArrayTupleSource(1);
        List<?> tuple = tupleSource.nextTuple();
        assertEquals(1, tuple.get(0));
        tupleSource.closeSource();
    }
}

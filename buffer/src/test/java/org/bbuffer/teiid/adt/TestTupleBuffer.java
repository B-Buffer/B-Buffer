package org.bbuffer.teiid.adt;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import org.bbuffer.BBuffer;
import org.bbuffer.TestHelper;
import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleBuffer.TupleBufferTupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

public class TestTupleBuffer {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }
    
    @Test
    public void testBasic() throws TeiidComponentException, TeiidProcessingException {
        
        ElementSymbol id = new ElementSymbol("id");
        id.setType(Integer.class);
        ElementSymbol name = new ElementSymbol("name");
        name.setType(String.class);
        List<ElementSymbol> elements = Arrays.asList(id, name);
        
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").build();
        bm.createTupleBuffer(elements, "Users1", TupleSourceType.PROCESSOR);
        TupleBuffer tb = bm.createTupleBuffer(elements, "Users", TupleSourceType.PROCESSOR);
        
        tb.setBatchSize(4);
        
        for(int i = 0 ; i < 10 ; i ++) {
            tb.addTuple(Arrays.asList(i, "name-" + i));
        }
        tb.setFinal(true);
        
        assertEquals(TupleBatch.NOT_TERMINATED, tb.getBatch(3).getTermination());
        assertEquals(TupleBatch.NOT_TERMINATED, tb.getBatch(6).getTermination());
        assertEquals(TupleBatch.TERMINATED, tb.getBatch(9).getTermination());

        TupleBufferTupleSource tupleSource = tb.createIndexedTupleSource();
        Set<String> names = new HashSet<>();
        while(tupleSource.hasNext()) {
            names.add((String)tupleSource.nextTuple().get(1));
        }
        tupleSource.closeSource();
        
        for(int i = 0 ; i < 10 ; i ++) {
            assertTrue(names.contains("name-" + i));
        }
    }
    
    @Test
    public void testForwardOnly() throws Exception {
        
        ElementSymbol id = new ElementSymbol("id", new GroupSymbol("Users"), Integer.class);
        ElementSymbol name = new ElementSymbol("name", new GroupSymbol("Users"), String.class);
        List<ElementSymbol> elements = Arrays.asList(id, name);
        
        TupleBuffer tb = BBuffer.Factory.builder().bufferDir("target/buffer").build().createTupleBuffer(elements, "Users", TupleSourceType.PROCESSOR);
        tb.setForwardOnly(true);
        tb.addTuple(Arrays.asList(1, "name"));
        tb.getBatch(1);
        
        try {
            tb.getBatch(1);
            fail("expected exception");
        } catch (AssertionError e) {
        } 
    }

}

package org.bbuffer.teiid.adt;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;

import org.bbuffer.BBuffer;
import org.bbuffer.TestHelper;
import org.junit.Test;
import org.teiid.client.ResizingArrayList;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.ExecutionFactory.NullOrder;

public class TestSTree {
    
    static {
        TestHelper.enableLogger(Level.ALL);
    }

    @Test
    public void testCreate() {
        
        ElementSymbol id = new ElementSymbol("id", new GroupSymbol("Users"), String.class);
        ElementSymbol name = new ElementSymbol("name", new GroupSymbol("Users"), String.class);  
        ElementSymbol phone = new ElementSymbol("phone", new GroupSymbol("Users"), String.class);
        ElementSymbol address = new ElementSymbol("address", new GroupSymbol("Users"), String.class);
        ElementSymbol country = new ElementSymbol("country", new GroupSymbol("Users"), String.class);
        
        List<ElementSymbol> elements = Arrays.asList(id, name, phone, address, country);
        
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").build();
        
        STree tree = bm.createSTree(elements, "Users", 2);
        bm.createSTree(elements, "Users", 2);
        
        assertEquals(1, tree.getHeight());
        assertEquals(2, tree.getKeyLength());
        assertEquals(0, tree.getRowCount());
        assertEquals(256, tree.getPageSize(true));//leafs size
        assertEquals(512, tree.getPageSize(false));//page size
    }
    
    @Test
    public void testCreateLeafSize() {
        
        ElementSymbol e1 = new ElementSymbol("e1", new GroupSymbol("X"), Integer.class);
        ElementSymbol e2 = new ElementSymbol("e2", new GroupSymbol("X"), String.class);
        ElementSymbol e3 = new ElementSymbol("e3", new GroupSymbol("X"), Boolean.class);
        ElementSymbol e4 = new ElementSymbol("e4", new GroupSymbol("X"), Byte.class);
        ElementSymbol e5 = new ElementSymbol("e5", new GroupSymbol("X"), Short.class);
        ElementSymbol e6 = new ElementSymbol("e6", new GroupSymbol("X"), Character.class);
        ElementSymbol e7 = new ElementSymbol("e7", new GroupSymbol("X"), Long.class);
        ElementSymbol e8 = new ElementSymbol("e8", new GroupSymbol("X"), BigInteger.class);
        ElementSymbol e9 = new ElementSymbol("e9", new GroupSymbol("X"), Float.class);
        ElementSymbol e10 = new ElementSymbol("e10", new GroupSymbol("X"), Double.class);
        ElementSymbol e11 = new ElementSymbol("e11", new GroupSymbol("X"), BigDecimal.class);
        ElementSymbol e12 = new ElementSymbol("e12", new GroupSymbol("X"), java.sql.Date.class);
        ElementSymbol e13 = new ElementSymbol("e13", new GroupSymbol("X"), java.sql.Time.class);
        ElementSymbol e14 = new ElementSymbol("e14", new GroupSymbol("X"), java.sql.Timestamp.class);
        ElementSymbol e15 = new ElementSymbol("e15", new GroupSymbol("X"), Object.class);
        
        List<ElementSymbol> elements = Arrays.asList(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15);
        List<ElementSymbol> elements1 = Arrays.asList(e2, e2, e2, e2, e2);
        List<ElementSymbol> elements2 = Arrays.asList(e1, e1);
        
        
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").build();
        
        // pageSize
        int pageSize = bm.getProcessorBatchSize(elements1.subList(0, 2));
        int leafSize = bm.getProcessorBatchSize(elements1);
        assertEquals(pageSize, 512);
        assertEquals(leafSize, 256);
        
        pageSize = bm.getProcessorBatchSize(elements2.subList(0, 1));
        leafSize = bm.getProcessorBatchSize(elements2);
     
        assertEquals(256, bm.getProcessorBatchSize(elements));
        assertEquals(2048, bm.getProcessorBatchSize(elements.subList(0, 1)));
        assertEquals(256, bm.getProcessorBatchSize(Arrays.asList(e2, e2, e2, e2)));
        
        bm = BBuffer.Factory.builder().bufferDir("target/buffer").processorBatchSize(4).build();
        assertEquals(4, bm.getProcessorBatchSize(Arrays.asList(e2, e2, e2, e2)));
    }
    
    @Test
    public void testCreateSmallTree() {
        ElementSymbol id = new ElementSymbol("id", new GroupSymbol("Users"), String.class);
        ElementSymbol name = new ElementSymbol("name", new GroupSymbol("Users"), String.class);  
        ElementSymbol phone = new ElementSymbol("phone", new GroupSymbol("Users"), String.class);
        ElementSymbol address = new ElementSymbol("address", new GroupSymbol("Users"), String.class);
        ElementSymbol country = new ElementSymbol("country", new GroupSymbol("Users"), String.class);
        
        List<ElementSymbol> elements = Arrays.asList(id, name, phone, address, country);
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").processorBatchSize(4).build();
        
        STree tree = bm.createSTree(elements, "Users", 2);
        
        assertEquals(4, bm.getProcessorBatchSize(elements));
        assertEquals(4, tree.getPageSize(true));//leafs size
        
        assertEquals(8, bm.getProcessorBatchSize(elements.subList(0,2)));
        assertEquals(8, tree.getPageSize(false));//page size
    }
    
    private STree sample() {
        ElementSymbol id = new ElementSymbol("id", new GroupSymbol("Users"), String.class);
        ElementSymbol name = new ElementSymbol("name", new GroupSymbol("Users"), String.class);  
        ElementSymbol phone = new ElementSymbol("phone", new GroupSymbol("Users"), String.class);
        ElementSymbol address = new ElementSymbol("address", new GroupSymbol("Users"), String.class);
        ElementSymbol country = new ElementSymbol("country", new GroupSymbol("Users"), String.class);
        
        List<ElementSymbol> elements = Arrays.asList(id, name, phone, address, country);
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").processorBatchSize(4).build();
        return bm.createSTree(elements, "Users", 2);
    }
    
    private STree sample1() {
        ElementSymbol id = new ElementSymbol("id", new GroupSymbol("Users"), String.class);
        ElementSymbol name = new ElementSymbol("name", new GroupSymbol("Users"), String.class);  
        ElementSymbol phone = new ElementSymbol("phone", new GroupSymbol("Users"), String.class);
        ElementSymbol address = new ElementSymbol("address", new GroupSymbol("Users"), String.class);
        ElementSymbol country = new ElementSymbol("country", new GroupSymbol("Users"), String.class);
        
        List<ElementSymbol> elements = Arrays.asList(id, name, phone, address, country);
        BufferManager bm = BBuffer.Factory.builder().bufferDir("target/buffer").build();
        return bm.createSTree(elements, "Users", 2);
    }
    
    @Test
    public void testHeight() throws TeiidComponentException {
        
        STree tree = sample1();
        
        for(int i = 1 ; i <= 1000 ; i ++) {
            tree.insert(Arrays.asList("" + i, "name-" + i, "123456789", "Beijing", "CN"), InsertMode.NEW, 1);
        }
        
        assertEquals(2, tree.getHeight());
    }
    
    @Test
    public void testInsert() throws TeiidComponentException, InterruptedException {
        
        STree tree = sample();
        
        for(int i = 1 ; i <= 32 ; i ++) {
            tree.insert(Arrays.asList("" + i, "name-" + i, "123456789", "Beijing", "CN"), InsertMode.NEW, 1);
        }

    }
    
    @Test
    public void testSearch() throws TeiidComponentException {
        
        STree tree = sample();
        for(int i = 1 ; i <= 32 ; i ++) {
            tree.insert(Arrays.asList("" + i, "name-" + i, "123456789", "Beijing", "CN"), InsertMode.NEW, 1);
        } 
        
        List<?> results = tree.find(Arrays.asList("7", "name-7"));
        assertEquals(5, results.size());
        assertEquals("CN", results.get(4));
        
        results = tree.find(Arrays.asList("13", "name-13"));
        assertEquals(5, results.size());
        assertEquals("CN", results.get(4));

    }
    
    @Test
    public void testBinarySearch() {
        
        ArrayList<String> list = new ArrayList<>();
        list.add("TP");
        list.add("PROVIDES");
        list.add("QUALITY");
        list.add("TUTORIALS");
        
        int index = Collections.binarySearch(list, "QUALITY");
        assertEquals(2, index);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testListNestedSortComparator() {
        
        List<List<?>> values = new ResizingArrayList<List<?>>();
        values.add(Arrays.asList("16", "name-16", "123456789", "Beijing", "CN"));
        values.add(Arrays.asList("30", "name-30", "123456789", "Beijing", "CN"));
        values.add(Arrays.asList("31", "name-31", "123456789", "Beijing", "CN"));
        values.add(Arrays.asList("5", "name-5", "123456789", "Beijing", "CN"));
        
        List<?> key = Arrays.asList("7", "name-7");
        
        int[] compareIndexes = new int[2];
        for (int i = 1; i < compareIndexes.length; i++) {
            compareIndexes[i] = i;
        }
        ListNestedSortComparator comparator = new ListNestedSortComparator(compareIndexes).defaultNullOrder(NullOrder.LOW);
        
        int index = Collections.binarySearch(values, key, comparator);
        assertEquals(-5, index);

    }
    
    @Test
    public void testXorshiftRNGs() {
        
        Random seedGenerator = new Random(0);
        int mask = 1;
        int shift = 1;
        int pageSize =8;
        int randomSeed = seedGenerator.nextInt() | 0x00000100; 
        pageSize >>>= 3;
        while (pageSize > 0) {
            pageSize >>>= 1;
            shift++;
            mask <<= 1;
            mask++;
        }
        
        // 1 -> 0
        // 2 -> 0
        // 3 -> 0
        // 4 -> 0
        // 5 -> 1
        // 6 -> 0
        // 7 -> 0
        // 8 -> 0
        // 9 -> 0
        // 10 -> 0
        // 11 -> 0
        // 12 -> 0
        // 13 -> 0
        // 14 -> 0
        // 15 -> 0
        // 16 -> 1
        // 17 -> 0
        // 18 -> 0
        // 19 -> 0
        // 20 -> 0
        // 21 -> 0
        // 22 -> 0
        // 23 -> 0
        // 24 -> 0
        // 25 -> 0
        // 26 -> 0
        // 27 -> 0
        // 28 -> 0
        // 29 -> 0
        // 30 -> 1
        // 31 -> 1
        // 32 -> 0
        for(int i = 1 ; i <= 32 ; i ++) {

            //compute level
            int x = randomSeed;
            x ^= x << 13;
            x ^= x >>> 17;
            randomSeed = x ^= x << 5;
            int level = 0;
            while ((x & mask) == mask) {
                ++level;
                x >>>= shift;
            }
            
            if(i == 5 || i == 16 || i == 30 || i == 31) {
                assertEquals(1, level);
            } else {
                assertEquals(0, level);
            }

        }
    }
    
    /**
     * Expected results
     * 
Level 1 
    2[[16, name-16]->7, [30, name-30]->1, [31, name-31]->14, [5, name-5]->3], 
Level 0 
    0[[1, name-1, 123456789, Beijing, CN] . 2 . [10, name-10, 123456789, Beijing, CN]], 
    5[[11, name-11, 123456789, Beijing, CN] . 2 . [12, name-12, 123456789, Beijing, CN]], 
    6[[13, name-13, 123456789, Beijing, CN] . 2 . [14, name-14, 123456789, Beijing, CN]], 
    7[[15, name-15, 123456789, Beijing, CN] . 2 . [16, name-16, 123456789, Beijing, CN]], 
    8[[17, name-17, 123456789, Beijing, CN] . 2 . [18, name-18, 123456789, Beijing, CN]], 
    9[[19, name-19, 123456789, Beijing, CN] . 2 . [2, name-2, 123456789, Beijing, CN]], 
    10[[20, name-20, 123456789, Beijing, CN] . 2 . [21, name-21, 123456789, Beijing, CN]], 
    11[[22, name-22, 123456789, Beijing, CN] . 2 . [23, name-23, 123456789, Beijing, CN]], 
    12[[24, name-24, 123456789, Beijing, CN] . 2 . [25, name-25, 123456789, Beijing, CN]], 
    13[[26, name-26, 123456789, Beijing, CN] . 4 . [29, name-29, 123456789, Beijing, CN]], 
    1[[3, name-3, 123456789, Beijing, CN] . 2 . [30, name-30, 123456789, Beijing, CN]], 
    14[[31, name-31, 123456789, Beijing, CN] . 3 . [4, name-4, 123456789, Beijing, CN]], 
    3[[5, name-5, 123456789, Beijing, CN] . 2 . [6, name-6, 123456789, Beijing, CN]], 
    4[[7, name-7, 123456789, Beijing, CN] . 3 . [9, name-9, 123456789, Beijing, CN]], 
     * @throws TeiidComponentException
     */
    @Test
    public void testToString() throws TeiidComponentException {

        STree tree = sample();
        for(int i = 1 ; i <= 32 ; i ++) {
            tree.insert(Arrays.asList("" + i, "name-" + i, "123456789", "Beijing", "CN"), InsertMode.NEW, 1);
        }
        System.out.println(tree);
        assertNotNull(tree.toString());
    }
    
    @Test
    public void testBasic_1() {
        
        boolean a = false;
        boolean b = true;
        // &
        assertEquals(false, a & b);
        assertEquals(true, !a & b);
        assertEquals(false, a & !b);
        // |
        assertEquals(true, a | b);
        assertEquals(true, !a | b);
        assertEquals(false, a | !b);
        assertEquals(true, !a | !b);
        // ^
        assertEquals(true, a ^ b);
        assertEquals(false, !a ^ b);
        assertEquals(false, a ^ !b);
        assertEquals(true, !a ^ !b);
        
        Random seedGenerator = new Random(0);
        for(int i = 0 ; i < 10 ; i ++) {
            int seed = seedGenerator.nextInt();
            int randomSeed = seed | 0x00000100;
            assertFalse(0 == randomSeed);
        }        
    }
    
    @Test
    public void testBasic_2() {
    
        assertEquals("11111111111111111111111111111111", Integer.toBinaryString(-1));
        assertEquals("11111111111111111111111111111111", Integer.toBinaryString(-1 >> 8));
        assertEquals("111111111111111111111111", Integer.toBinaryString(-1 >>> 8));
    }
}

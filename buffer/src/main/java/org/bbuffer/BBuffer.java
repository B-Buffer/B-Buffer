package org.bbuffer;

public interface BBuffer {
    
    public static class Factory {
        
        public static BufferManagerBuilder getBufferManagerBuilder() {
            return new BufferManagerBuilder();
        }
    }

}

package org.bbuffer;

public interface BBuffer {
    
    public static class Factory {
        
        public static BufferBuilder getBuilder() {
            return new BufferBuilder();
        }
    }

}

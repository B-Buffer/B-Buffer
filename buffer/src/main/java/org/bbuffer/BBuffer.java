package org.bbuffer;

public interface BBuffer {
    
    public static class Factory {
        
        public static BufferBuilder builder() {
            return new BufferBuilder();
        }
    }

}

package org.bbuffer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.StorageManager;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.EncryptedStorageManager;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.FileUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;


public class BufferBuilder {
    
    private static final long MB = 1<<20;
    private static final long GB = 1<<30;

    // Instance
    private BufferManagerImpl bufferMgr;
    private File bufferDir;
    private boolean useDisk = true;
    private boolean encryptFiles = false;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private int maxOpenFiles = FileStorageManager.DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = SplittableStorageManager.DEFAULT_MAX_FILESIZE; // 2GB
    private int maxProcessingKb = BufferManager.DEFAULT_MAX_PROCESSING_KB;
    private int maxReserveKb = BufferManager.DEFAULT_RESERVE_BUFFER_KB;
    private long maxBufferSpace = FileStorageManager.DEFAULT_MAX_BUFFERSPACE>>20;
    private boolean inlineLobs = true;
    private long memoryBufferSpace = -1;
    private int maxStorageObjectSize = BufferFrontedFileStoreCache.DEFAuLT_MAX_OBJECT_SIZE;
    private boolean memoryBufferOffHeap;
    private FileStorageManager fsm;
    private BufferFrontedFileStoreCache fsc;
    protected int workingMaxReserveKb;
    
    public BufferBuilder processorBatchSize(int processorBatchSize) {
        this.processorBatchSize = processorBatchSize;
        return this;
    }
    
    public BufferBuilder maxReserveKb(int maxReserveKb) {
        this.maxReserveKb = maxReserveKb;
        return this;
    }
    
    public BufferBuilder maxProcessingKb(int maxProcessingKb) {
        this.maxProcessingKb = maxProcessingKb;
        return this;
    }
    
    public BufferBuilder inlineLobs(boolean inlineLobs) {
        this.inlineLobs = inlineLobs;
        return this;
    }
    
    public BufferBuilder useDisk(boolean useDisk) {
        this.useDisk = useDisk;
        return this;
    }
    
    public BufferBuilder bufferDir (String bufferDir) {
        Path path = Paths.get(bufferDir, "buffer");
        try {
            this.bufferDir =  Files.createDirectories(path).toFile();
        } catch (IOException e) {
        }
        return this;
    }
    
    public BufferBuilder maxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }
    
    public BufferBuilder maxBufferSpace(long maxBufferSpace) {
        this.maxBufferSpace = maxBufferSpace;
        return this;
    }
    
    public BufferBuilder maxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }
    
    public BufferBuilder encryptFiles(boolean encryptFiles) {
        this.encryptFiles = encryptFiles;
        return this;
    }
    
    public BufferBuilder maxStorageObjectSize(int maxStorageObjectSize) {
        this.maxStorageObjectSize = maxStorageObjectSize;
        return this;
    }
    
    public BufferBuilder memoryBufferOffHeap(boolean memoryBufferOffHeap) {
        this.memoryBufferOffHeap = memoryBufferOffHeap;
        return this;
    }
    
    public BufferBuilder memoryBufferSpace(long memoryBufferSpace) {
        this.memoryBufferSpace = memoryBufferSpace;
        return this;
    }
    
    public BufferManager build() {
        
        try {
            // Construct and initialize the buffer manager
            this.bufferMgr = new BufferManagerImpl();
            this.bufferMgr.setProcessorBatchSize(processorBatchSize);
            this.bufferMgr.setMaxReserveKB(this.maxReserveKb);
            this.bufferMgr.setMaxProcessingKB(this.maxProcessingKb);
            this.bufferMgr.setInlineLobs(inlineLobs);
            this.bufferMgr.initialize();
            
             // If necessary, add disk storage manager
            if(useDisk) {
                LogManager.logDetail(LogConstants.CTX_DQP, "Starting BufferManager using", bufferDir);
                if(!bufferDir.exists()) {
                    this.bufferDir.mkdir();
                }
                
                cleanDirectory(bufferDir);

                // Get the properties for FileStorageManager and create.
                fsm = new FileStorageManager();
                fsm.setStorageDirectory(bufferDir.getCanonicalPath());
                fsm.setMaxOpenFiles(maxOpenFiles);
                fsm.setMaxBufferSpace(maxBufferSpace * MB);
                SplittableStorageManager ssm = new SplittableStorageManager(fsm);
                ssm.setMaxFileSize(maxFileSize);
                StorageManager sm = ssm;
                if (encryptFiles){
                    sm = new EncryptedStorageManager(ssm);
                }
                fsc = new BufferFrontedFileStoreCache();
                fsc.setBufferManager(this.bufferMgr);
                fsc.setMaxStorageObjectSize(maxStorageObjectSize);
                fsc.setDirect(memoryBufferOffHeap);
            
                //use approximately 40% of what's set aside for the reserved accounting for conversion from kb to bytes
                long autoMaxBufferSpace = 4*(((long)this.bufferMgr.getMaxReserveKB())<<10)/10;
            
                // estimate inode/batch overhead
                if (memoryBufferSpace < 0) {
                    fsc.setMemoryBufferSpace(autoMaxBufferSpace);
                } else {
                    //scale from MB to bytes
                    fsc.setMemoryBufferSpace(memoryBufferSpace << 20);
                }
            
                long batchAndInodeOverheadKB = fsc.getMemoryBufferSpace()>>(memoryBufferOffHeap?19:17);
                this.bufferMgr.setMaxReserveKB((int)Math.max(0, this.bufferMgr.getMaxReserveKB() - batchAndInodeOverheadKB));
                if (this.maxReserveKb < 0) {
                    if (memoryBufferOffHeap) {
                        this.bufferMgr.setMaxReserveKB(8*this.bufferMgr.getMaxReserveKB()/10);
                    } else {
                        this.bufferMgr.setMaxReserveKB((int)Math.max(0, this.bufferMgr.getMaxReserveKB() - (fsc.getMemoryBufferSpace()>>10)));
                    }
                }
                fsc.setStorageManager(sm);
                fsc.initialize();
                this.bufferMgr.setCache(fsc);
                this.workingMaxReserveKb = this.bufferMgr.getMaxReserveKB();
            } else {
                MemoryStorageManager msm = new MemoryStorageManager();
                SplittableStorageManager ssm = new SplittableStorageManager(msm);
                ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
                this.bufferMgr.setCache(msm);
                this.bufferMgr.setStorageManager(ssm);
            }
                        
        } catch (TeiidComponentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        
        return this.bufferMgr;
    }
    
    private void cleanDirectory(File file) {
        FileUtils.removeChildrenRecursively(file);
    }
  
}

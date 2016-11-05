package org.bbuffer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.teiid.logging.MessageLevel;

public class TestHelper {

    public static void enableLogger(Level level) {
        enableLogger(level, "org.teiid", "org.bbuffer"); //$NON-NLS-1$
    }
    

    public static void enableLogger(Level level, String... names){
        enableLogger(new TestLoggerFormatter(), Level.SEVERE, level, names);
    }
    
    public static void enableLogger(Formatter formatter, Level rootLevel, Level level, String... names){
        
        Logger rootLogger = Logger.getLogger("");
        for(Handler handler : rootLogger.getHandlers()){
            handler.setFormatter(formatter);
            handler.setLevel(rootLevel);
        }
        
        for(String name : names) {
            Logger logger = Logger.getLogger(name);
            logger.setLevel(level);
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(level);
            handler.setFormatter(formatter);
            logger.addHandler(handler);
            logger.setUseParentHandlers(false);
        }
        
        org.teiid.logging.LogManager.isMessageToBeRecorded("org.teiid", MessageLevel.INFO);
    }
    
    public static class TestLoggerFormatter extends Formatter {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm SSS"); //$NON-NLS-1$ 

        public String format(LogRecord record) {
            StringBuffer sb = new StringBuffer();
            sb.append(format.format(new Date(record.getMillis())) + " "); //$NON-NLS-1$ 
            sb.append(getLevelString(record.getLevel()) + " "); //$NON-NLS-1$ 
            sb.append("[" + record.getLoggerName() + "] ("); //$NON-NLS-1$ //$NON-NLS-2$  
            sb.append(Thread.currentThread().getName() + ") "); //$NON-NLS-1$ 
            sb.append(record.getMessage() + "\n"); //$NON-NLS-1$ 
            return sb.toString();
        }

        private String getLevelString(Level level) {
            String name = level.toString();
            int size = name.length();
            for(int i = size; i < 7 ; i ++){
                name += " ";
            }
            return name;
        }
    }
}

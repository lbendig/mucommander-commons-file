package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.FSDataOutputStream
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class FSDataOutputStream {

    private static final Class clazz;
    
    private final Object fsDataOutputStream;
    
    static {
        try {
            clazz = loadHadoopClass("org.apache.hadoop.fs.FSDataOutputStream");
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize FSDataOutputStream wrapper!", e);
        }
    }
    
    FSDataOutputStream(Object fsDatInputStream) {
        this.fsDataOutputStream = fsDatInputStream;
    }
    
    public static Class getClassToken() {
        return clazz;
    }
    
    /**
     * @return The instantiated FsDataOutputStream object it wraps
     */
    public Object getFsDataOutputStream() {
        return fsDataOutputStream;
    }
}

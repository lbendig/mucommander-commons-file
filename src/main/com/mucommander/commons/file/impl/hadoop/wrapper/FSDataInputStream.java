package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.FSDataInputStream
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class FSDataInputStream {

    private static final Class clazz;
    private static final Method method_getPos;
    private static final Method method_seek;
    private static final Method method_read1;
    private static final Method method_read2;
    
    private final Object fsDataInputStream;
    
    static {
        try {
            clazz = loadHadoopClass("org.apache.hadoop.fs.FSDataInputStream");
            method_getPos = getMethod(clazz, "getPos");
            method_seek = getMethod(clazz,"seek", long.class);
            method_read1 = getMethod(clazz.getSuperclass().getSuperclass(),"read");
            method_read2 = getMethod(clazz.getSuperclass(),"read", byte[].class, int.class, int.class);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize FSDataInputStream wrapper!", e);
        }
    }
    
    
    FSDataInputStream(Object fsDatInputStream) {
        this.fsDataInputStream = fsDatInputStream;
    }
    
    public static Class getClassToken() {
        return clazz;
    }
    
    /**
     * @return The instantiated FSDataInputStream object it wraps
     */
    public Object getFsDataInputStream() {
        return fsDataInputStream;
    }

    public long getPos() throws IOException {
        try {
            return (Long) method_getPos.invoke(fsDataInputStream);
        }
        catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }

    public void seek(long desired) throws IOException {
        try {
            method_seek.invoke(fsDataInputStream, desired);
        }
        catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }
    
    public int read() throws IOException {
        try {
            return (Integer) method_read1.invoke(fsDataInputStream);
        }
        catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }
    
    public int read(byte[] buffer, int offset, int length) throws IOException {
        try {
            return (Integer) method_read2.invoke(fsDataInputStream, buffer, offset, length);
        }
        catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }
    
}

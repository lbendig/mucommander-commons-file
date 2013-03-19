package com.mucommander.commons.file.impl.qfs.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadQfsClass;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Reflection-based wrapper for com.quantcast.qfs.access.KfsInputChannel.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class KfsInputChannel {

    private static final Class clazz;
    private static final Method method_tell;
    private static final Method method_seek;
    private static final Method method_read;
    private static final Method method_close;
    
    private final Object kfsInputChannel;
    
    static {
        try {

            clazz = loadQfsClass("com.quantcast.qfs.access.KfsInputChannel");
            method_tell = getMethod(clazz, "tell");
            method_seek = getMethod(clazz, "seek", long.class);
            method_read = getMethod(clazz, "read", ByteBuffer.class);
            method_close = getMethod(clazz, "close");
    
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize KfsInputChannel wrapper!", e);
        }
    }

    KfsInputChannel(Object kfsInputChannel) {
        this.kfsInputChannel = kfsInputChannel;
    }

    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated KfsInputChannel object it wraps
     */
    public Object getKfsInputChannel() {
        return kfsInputChannel;
    }
    
    public long tell() throws IOException {
        try {
            return (Long) method_tell.invoke(kfsInputChannel);
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
    
    public long seek(long offset) throws IOException {
        try {
            return (Long) method_seek.invoke(kfsInputChannel, offset);
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
    
    public int read(ByteBuffer dst) throws IOException {
        try {
            return (Integer) method_read.invoke(kfsInputChannel, dst);
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
    
    public void close() throws IOException {
        try {
            method_close.invoke(kfsInputChannel);
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

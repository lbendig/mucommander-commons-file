package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.permission.FsPermission
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class FsPermission {

    private static final Class clazz;
    private static final Method method_getDefault;
    private static final Method method_applyUMask;
    private static final Method method_getUMask;
    private static final Method method_toShort;
    
    private Object fsPermission;
    
    static {

        try {
            clazz = loadHadoopClass("org.apache.hadoop.fs.permission.FsPermission");
            method_getDefault = getMethod(clazz, "getDefault");
            method_applyUMask = getMethod(clazz, "applyUMask", FsPermission.getClassToken());
            method_getUMask = getMethod(clazz, "getUMask", Configuration.getClassToken());
            method_toShort = getMethod(clazz, "toShort");
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize FsPermission wrapper!", e);
        }
        
    }
    
    public FsPermission(short mode) {
        try {
            Constructor constr = clazz.getConstructor(short.class);
            fsPermission = constr.newInstance(mode);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    FsPermission(Object fsPermission) {
        this.fsPermission = fsPermission;
    }

    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated FsPermission object it wraps
     */
    public Object getFsPermission() {
        return fsPermission;
    }
  
    /** Get the default permission. */
    public static FsPermission getDefault() {
        try {
            return new FsPermission(method_getDefault.invoke(null));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /** Create an immutable {@link FsPermission} object. */
    public FsPermission applyUMask(FsPermission umask) {
        try {
            return new FsPermission(
                    method_applyUMask.invoke(fsPermission, umask.getFsPermission()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /** 
     * Get the user file creation mask (umask)
     * 
     * {@code UMASK_LABEL} config param has umask value that is either symbolic 
     * or octal.
     * 
     * Symbolic umask is applied relative to file mode creation mask; 
     * the permission op characters '+' clears the corresponding bit in the mask, 
     * '-' sets bits in the mask.
     * 
     * Octal umask, the specified bits are set in the file mode creation mask.
     * 
     * {@code DEPRECATED_UMASK_LABEL} config param has umask value set to decimal.
     */
    public static FsPermission getUMask(Configuration conf) {
        try {
            return new FsPermission(method_getUMask.invoke(null, conf.getConfiguration()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Encode the object to a short.
     */
    public short toShort() {
        try {
            return (Short)method_toShort.invoke(fsPermission);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}

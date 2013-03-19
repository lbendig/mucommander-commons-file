package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.FileStatus.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class FileStatus {

    private static final Class clazz;
    private static final Method method_getPath;
    private static final Method method_isDir;
    private static final Method method_getModificationTime;
    private static final Method method_getLen;
    private static final Method method_getPermission;
    private static final Method method_getOwner;
    private static final Method method_getGroup;
    
    private Object fileStatus;
    
    static {
        try {
            
            clazz = loadHadoopClass("org.apache.hadoop.fs.FileStatus");
            method_getPath = getMethod(clazz, "getPath");
            method_isDir = getMethod(clazz, "isDir");
            method_getModificationTime = getMethod(clazz, "getModificationTime");
            method_getLen = getMethod(clazz, "getLen");
            method_getPermission = getMethod(clazz, "getPermission");
            method_getOwner = getMethod(clazz, "getOwner");
            method_getGroup = getMethod(clazz, "getGroup");
            
        }
        catch (Exception e) {
            throw new RuntimeException("Can't initialize FileStatus wrapper!", e);
        }
    }
    
    public FileStatus() {
        try {
            fileStatus = clazz.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    FileStatus(Object fileStatus) {
        this.fileStatus = fileStatus;
    }
    
    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated FileStatus object it wraps
     */
    public Object getFileStatus() {
        return fileStatus;
    }
    
    public Path getPath() {
        try {
            return new Path(method_getPath.invoke(fileStatus));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Is this a directory?
     * @return true if this is a directory
     */
    public boolean isDir() {
        try {
            return (Boolean)method_isDir.invoke(fileStatus);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Get the modification time of the file.
     * @return the modification time of file in milliseconds since January 1, 1970 UTC.
     */
    public long getModificationTime() {
        try {
            return (Long)method_getModificationTime.invoke(fileStatus);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* 
     * @return the length of this file, in blocks
     */
    public long getLen() {
        try {
            return (Long)method_getLen.invoke(fileStatus);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Get FsPermission associated with the file.
     * @return permssion. If a filesystem does not have a notion of permissions
     *         or if permissions could not be determined, then default 
     *         permissions equivalent of "rwxrwxrwx" is returned.
     */
    public FsPermission getPermission() {
        try {
            return new FsPermission(method_getPermission.invoke(fileStatus));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Get the owner of the file.
     * @return owner of the file. The string could be empty if there is no
     *         notion of owner of a file in a filesystem or if it could not 
     *         be determined (rare).
     */
    public String getOwner() {
        try {
            return (String) method_getOwner.invoke(fileStatus);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Get the group associated with the file.
     * @return group for the file. The string could be empty if there is no
     *         notion of group of a file in a filesystem or if it could not 
     *         be determined (rare).
     */
    public String getGroup() {
        try {
            return (String) method_getGroup.invoke(fileStatus);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

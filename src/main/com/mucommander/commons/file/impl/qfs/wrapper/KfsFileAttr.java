package com.mucommander.commons.file.impl.qfs.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.loadQfsClass;

import java.lang.reflect.Field;

/**
 * Reflection-based wrapper for com.quantcast.qfs.access.KfsFileAttr.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class KfsFileAttr {

    private static final Class clazz;
    private static final Field field_filename;
    private static final Field field_isDirectory;
    private static final Field field_modificationTime;
    private static final Field field_filesize;
    private static final Field field_mode;
    private static final Field field_ownerName;
    private static final Field field_groupName;
    
    private Object kfsFileAttr;

    static {
        try {

            clazz = loadQfsClass("com.quantcast.qfs.access.KfsFileAttr");
            field_filename = clazz.getDeclaredField("filename");
            field_isDirectory = clazz.getDeclaredField("isDirectory");
            field_modificationTime = clazz.getDeclaredField("modificationTime");
            field_filesize = clazz.getDeclaredField("filesize");
            field_mode = clazz.getDeclaredField("mode");
            field_ownerName = clazz.getDeclaredField("ownerName");
            field_groupName = clazz.getDeclaredField("groupName");
            
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize KfsFileAttr wrapper!", e);
        }
    }
    
    public KfsFileAttr() {
        try {
            this.kfsFileAttr = clazz.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    KfsFileAttr(Object kfsFileAttr) {
        this.kfsFileAttr = kfsFileAttr;
    }
    
    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated KfsFileAttr object it wraps
     */
    public Object getKfsFileAttr() {
        return kfsFileAttr;
    }

    public String getFilename() {
        try {
            return (String)field_filename.get(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean getIsDirectory() {
        try {
            return field_isDirectory.getBoolean(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public long getModificationTime() {
        try {
            return field_modificationTime.getLong(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public long getFilesize() {
        try {
            return field_filesize.getLong(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getMode() {
        try {
            return field_mode.getInt(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public String getOwnerName() {
        try {
            return (String) field_ownerName.get(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public String getGroupName() {
        try {
            return (String) field_groupName.get(kfsFileAttr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }    
}

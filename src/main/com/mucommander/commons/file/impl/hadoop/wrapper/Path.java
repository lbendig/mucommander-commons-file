package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.Path
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Path {

    private static final Class clazz;
    private static final Method method_getName;

    private final Object path;

    static {

        try {
            clazz = loadHadoopClass("org.apache.hadoop.fs.Path");
            method_getName = getMethod(clazz, "getName");
        }
        catch (Exception e) {
            throw new RuntimeException("Can't initialize Path wrapper!", e);
        }

    }
    
    public Path(String pathString) {
        try {
            Constructor constr = clazz.getConstructor(String.class);
            path = constr.newInstance(pathString);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    Path(Object path) {
        this.path = path;
    }

    public static Class getClassToken() {
        return clazz;
    }
    
    /**
     * @return The instantiated Path object it wraps
     */
    public Object getPath() {
        return path;
    }

    /** Returns the final component of this path.*/
    public String getName() {
        try {
            return (String) method_getName.invoke(path);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}


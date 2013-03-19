package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.conf.Configuration.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class Configuration {

    private static final Class clazz;
    private static final Method method_get1;
    private static final Method method_set1;

    private final Object configuration;

    static {
        try {
            clazz = loadHadoopClass("org.apache.hadoop.conf.Configuration");
            
            method_get1 = getMethod(clazz, "get", String.class, String.class);
            method_set1 = getMethod(clazz, "setStrings", String.class, String[].class);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize Configuration wrapper!", e);
        }
    }

    public Configuration() {
        try {
            configuration = clazz.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated Configuration object it wraps
     */
    public Object getConfiguration() {
        return configuration;
    }

    /** 
     * Get the value of the <code>name</code> property. If no such property 
     * exists, then <code>defaultValue</code> is returned.
     * 
     * @param name property name.
     * @param defaultValue default value.
     * @return property value, or <code>defaultValue</code> if the property 
     *         doesn't exist.                    
     */
    public String get(String name, String defaultValue) {
        try {
            return (String) method_get1.invoke(configuration, name, defaultValue);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 
     * Set the array of string values for the <code>name</code> property as 
     * as comma delimited values.  
     * 
     * @param name property name.
     * @param values The values
     */
    public void setStrings(String name, String... values) {
        try {
            method_set1.invoke(configuration, name, values);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

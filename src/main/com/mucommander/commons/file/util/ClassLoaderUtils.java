package com.mucommander.commons.file.util;

import java.lang.reflect.Method;

import com.mucommander.commons.file.FileFactory;
import com.mucommander.commons.file.FileProtocols.CustomLoadableProtocol;

/**
 * Reflection-based class loading utilities.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public final class ClassLoaderUtils {

    private ClassLoaderUtils() {
        throw new AssertionError("shouldn't be instantiated");
    }

    /**
     * Returns the Method object for a given method name and parameter list.
     * @param clazz Class object
     * @param name method name
     * @param parameterTypes parameter type list
     * @return Method object
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public static Method getMethod(Class<?> clazz, String name, Class<?>... parameterTypes)
            throws NoSuchMethodException, SecurityException {
        return clazz.getDeclaredMethod(name, parameterTypes);
    }

    /**
     * Loads a class with a provided classLoader
     * 
     * @param className fully qualified class name
     * @param classloader the clasloader to be used
     * @return the initialized Class object
     * @throws ClassNotFoundException
     */
    public static Class<?> loadClass(String className, ClassLoader classloader) throws ClassNotFoundException {
        return Class.forName(className, true, classloader);
    }
    
    /**
     * A convenient method to load Hadoop related classes.
     * 
     * @param className fully qualified class name
     * @return Class object initialized by the Hadoop classloader
     * @throws ClassNotFoundException
     */
    public static Class<?> loadHadoopClass(String className) throws ClassNotFoundException {
        return loadClass(className, FileFactory.getCustomClassLoader(CustomLoadableProtocol.HDFS));
    }
    
    /**
     * A convenient method to load QFS related classes.
     * 
     * @param className fully qualified class name
     * @return Class object initialized by the QFS classloader
     * @throws ClassNotFoundException
     */
    public static Class<?> loadQfsClass(String className) throws ClassNotFoundException {
        return loadClass(className, FileFactory.getCustomClassLoader(CustomLoadableProtocol.QFS));
    }

}

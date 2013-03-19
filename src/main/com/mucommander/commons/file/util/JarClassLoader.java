package com.mucommander.commons.file.util;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.mucommander.commons.util.StringUtils;

/**
 * Loads classes from JAR files.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public final class JarClassLoader {

    /** Extension to be considered */
    private static final String EXTENSION = ".jar";
    
    private static FileFilter filter = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            if (pathname.isDirectory()) {
                return true;
            }
            return StringUtils.endsWithIgnoreCase(pathname.getName(), EXTENSION);
        }
    };

    private JarClassLoader() {
        throw new AssertionError("shouldn't be instantiated");
    }

    /**
     * Loads classes from the provided path.
     * 
     * @param path - can be either a directory or a jar file
     * @return a {@link URLClassLoader} instance 
     * @throws MalformedURLException 
     * @throws Exception
     */
    public static ClassLoader load(File path) throws MalformedURLException {
        Collection<URL> jars = getJarLocations(path);
        return new URLClassLoader(jars.toArray(new URL[jars.size()]));
    }
    
    private static Collection<URL> getJarLocations(File path) throws MalformedURLException {
        Map<String, URL> store = new HashMap<String, URL>();
        getJarLocations(path, store);
        return store.values();
    }

    private static void getJarLocations(File path, Map<String, URL> result)
            throws MalformedURLException {

        if (path.isFile()) {
            result.put(path.getName(), path.toURI().toURL());
            return;
        }

        File[] listFiles = path.listFiles(filter);
        if (listFiles == null) {
            return;
        }
        for (File file : listFiles) {
            if (file.isDirectory()) {
                getJarLocations(file, result);
            }
            else {
                result.put(file.getName(), file.toURI().toURL());
            }
        }
    }
}

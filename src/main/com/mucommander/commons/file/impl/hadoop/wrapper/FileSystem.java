package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * Reflection-based wrapper for org.apache.hadoop.fs.FileSystem
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public final class FileSystem {

    private static final Class clazz;
    private static final Method method_append;
    private static final Method method_create;
    private static final Method method_mkdirs;
    private static final Method method_delete;
    private static final Method method_rename;
    private static final Method method_setTimes;
    private static final Method method_open;
    private static final Method method_listStatus1;
    private static final Method method_listStatus2;
    private static final Method method_setPermission;
    private static final Method method_getFileStatus;
    private static final Method method_get;

    private Object fileSystem;

    static {
        try {

            clazz = loadHadoopClass("org.apache.hadoop.fs.FileSystem");
            
            method_append = getMethod(clazz, "append", Path.getClassToken());
            method_create = getMethod(clazz, "create", Path.getClassToken(), boolean.class);
            method_mkdirs = getMethod(clazz, "mkdirs", Path.getClassToken());
            method_delete = getMethod(clazz, "delete", Path.getClassToken(), boolean.class);
            method_rename = getMethod(clazz, "rename", Path.getClassToken(), 
                    Path.getClassToken());
            method_setTimes = getMethod(clazz, "setTimes", Path.getClassToken(), long.class, 
                    long.class);
            method_open = getMethod(clazz, "open", Path.getClassToken());
            method_listStatus1 = getMethod(clazz, "listStatus", Path.getClassToken());
            method_listStatus2 = getMethod(clazz, "listStatus", Path.getClassToken(), 
                    loadHadoopClass("org.apache.hadoop.fs.PathFilter"));
            method_setPermission = getMethod(clazz, "setPermission", Path.getClassToken(), 
                    FsPermission.getClassToken());
            method_getFileStatus = getMethod(clazz, "getFileStatus", Path.getClassToken());
            method_get = getMethod(clazz, "get", URI.class, Configuration.getClassToken());

        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize FileSystem wrapper!", e);
        }
    }

    private FileSystem() {

    }

    public static FileSystem get(final URI uri, final Configuration conf) {
        FileSystem fs = new FileSystem();
        try {
            fs.fileSystem = method_get.invoke(null, uri, conf.getConfiguration());
            return fs;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated FileSystem object it wraps
     */
    public Object getFileSystem() {
        return fileSystem;
    }

    /**
     * Append to an existing file (optional operation).
     * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
     * @param f the existing file to be appended.
     * @throws IOException
     */
    public FSDataOutputStream append(Path f) throws IOException {
        try {
            return new FSDataOutputStream(method_append.invoke(fileSystem, f.getPath()));
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

    /**
     * Opens an FSDataOutputStream at the indicated Path.
     */
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        try {
            return new FSDataOutputStream(method_create.invoke(fileSystem, f.getPath(), overwrite));
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

    /** 
     * Create a directory with default provided permission.

     * @param f the name of the directory to be created
     * @return true if the directory creation succeeds; false otherwise
     * @throws IOException
     */
    public boolean mkdirs(Path f) throws IOException {
        try {
            return (Boolean) method_mkdirs.invoke(fileSystem, f.getPath());
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

    /** Delete a file.
    *
    * @param f the path to delete.
    * @param recursive if path is a directory and set to 
    * true, the directory is deleted else throws an exception. In
    * case of a file the recursive can be set to either true or false. 
    * @return  true if delete is successful else false. 
    * @throws IOException
    */
    public boolean delete(Path f, boolean recursive) throws IOException {
        try {
            return (Boolean) method_delete.invoke(fileSystem, f.getPath(), recursive);
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

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     */
    public boolean rename(Path src, Path dst) throws IOException {
        try {
            return (Boolean) method_rename.invoke(fileSystem, src.getPath(), dst.getPath());
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

    /**
     * Set access time of a file
     * @param p The path
     * @param mtime Set the modification time of this file.
     *              The number of milliseconds since Jan 1, 1970. 
     *              A value of -1 means that this call should not set modification time.
     * @param atime Set the access time of this file.
     *              The number of milliseconds since Jan 1, 1970. 
     *              A value of -1 means that this call should not set access time.
     */
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        try {
            method_setTimes.invoke(fileSystem, p.getPath(), mtime, atime);
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

    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file to open
     */
    public FSDataInputStream open(Path f) throws IOException {
        try {
            return new FSDataInputStream(method_open.invoke(fileSystem, f.getPath()));
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

    /**
     * List the statuses of the files/directories in the given path if the path is
     * a directory.
     * 
     * @param f
     *          given path
     * @return the statuses of the files/directories in the given patch
     *         returns null, if Path f does not exist in the FileSystem
     * @throws IOException
     */
    public FileStatus[] listStatus(Path f) throws IOException {
        try {
            Object[] objs = (Object[]) method_listStatus1.invoke(fileSystem, f.getPath());
            if (objs == null) {
                return null;
            }
            FileStatus[] result = new FileStatus[objs.length];
            for (int i = 0; i != objs.length; i++) {
                result[i] = new FileStatus(objs[i]);
            }
            return result;
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

    /**
     * Filter files/directories in the given path using the user-supplied path
     * filter.
     * 
     * @param f
     *          a path name
     * @param filter
     *          the user-supplied path filter
     * @return an array of FileStatus objects for the files under the given path
     *         after applying the filter
     * @throws IOException
     *           if encounter any problem while fetching the status
     */
    public FileStatus[] listStatus(Path f, Object filter) throws IOException {
        try {
            Object[] objs = (Object[]) method_listStatus2.invoke(
                    fileSystem, f.getPath(), filter/*PathFilter*/);
            if (objs == null) {
                return null;
            }
            FileStatus[] result = new FileStatus[objs.length];
            for (int i = 0; i != objs.length; i++) {
                result[i] = new FileStatus(objs[i]);
            }
            return result;
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

    /**
     * Set permission of a path.
     * @param p
     * @param permission
     */
    public void setPermission(Path p, int permission) throws IOException {
        try {
            method_setPermission.invoke(fileSystem, p.getPath(), new FsPermission(
                    (short) permission).getFsPermission());
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

    /**
     * Return a file status object that represents the path.
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist;
     *         IOException see specific implementation
     */
    public FileStatus getFileStatus(Path f) throws IOException {
        try {
            return new FileStatus(method_getFileStatus.invoke(fileSystem, f.getPath()));
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

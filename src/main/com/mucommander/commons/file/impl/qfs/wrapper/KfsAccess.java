package com.mucommander.commons.file.impl.qfs.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadQfsClass;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.Permission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reflection-based wrapper for com.quantcast.qfs.access.KfsAccess.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KfsAccess {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KfsAccess.class);

    private static final Class clazz;
    private static final Method method_kfs_append_ex;
    private static final Method method_kfs_create;
    private static final Method method_kfs_isDirectory;
    private static final Method method_kfs_mkdirs;
    private static final Method method_kfs_rmdirs;
    private static final Method method_kfs_remove;
    private static final Method method_kfs_rename;
    private static final Method method_kfs_setModificationTime;
    private static final Method method_kfs_stat;
    private static final Method method_kfs_readdirplus;
    private static final Method method_kfs_retToIOException;
    private static final Method method_kfs_chmod;
    private static final Method method_kfs_open;
    
    private Object kfsAccess;
    
    static {
        SystemExitManager sem = null;
        try {
            
            // The static initializer of com.quantcast.qfs.access.KfsAccess terminates 
            // the JVM if the native library it uses cannot be loaded. To prevent from exiting
            // from muCommander a temporary SecurityManager is applied during the class loading 
            // which prohibits the JVM termination
            sem = new SystemExitManager();
            System.setSecurityManager(sem);
            clazz = loadQfsClass("com.quantcast.qfs.access.KfsAccess");
            
            method_kfs_append_ex = getMethod(clazz, "kfs_append_ex", String.class, int.class, int.class);
            method_kfs_create = getMethod(clazz, "kfs_create", String.class, int.class, boolean.class, long.class, long.class);
            method_kfs_isDirectory = getMethod(clazz, "kfs_isDirectory", String.class);
            method_kfs_mkdirs = getMethod(clazz, "kfs_mkdirs", String.class);
            method_kfs_rmdirs = getMethod(clazz, "kfs_rmdirs", String.class);
            method_kfs_remove = getMethod(clazz, "kfs_remove", String.class);
            method_kfs_rename = getMethod(clazz, "kfs_rename", String.class, String.class);
            method_kfs_setModificationTime = getMethod(clazz, "kfs_setModificationTime", String.class, long.class);
            method_kfs_stat = getMethod(clazz, "kfs_stat", String.class, KfsFileAttr.getClassToken());
            method_kfs_readdirplus = getMethod(clazz, "kfs_readdirplus", String.class);
            method_kfs_retToIOException = getMethod(clazz, "kfs_retToIOException", int.class, String.class);
            method_kfs_chmod = getMethod(clazz, "kfs_chmod", String.class, int.class);
            method_kfs_open = getMethod(clazz, "kfs_open", String.class);

        }
        catch (LinkageError e) {
            throw new RuntimeException("Couldn't load QFS native library!", e);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize KfsAccess wrapper!", e);
        }
        finally {
            System.setSecurityManager(sem.getOriginalSecurityManager());
        }
    }

    public KfsAccess(String metaServerHost, int metaServerPort) throws IOException {
        checkServer(metaServerHost, metaServerPort);
        try {
            Constructor constr = clazz.getConstructor(String.class, int.class);
            this.kfsAccess = constr.newInstance(metaServerHost, metaServerPort);
        }
        catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new RuntimeException(e);
        }
    }
    
    /**
     * SecurityManager that caches System.exit and throws a SecurityException instead.
     * 
     * @author Lorand Bendig <lbendig@gmail.com>
     *
     */
    private static class SystemExitManager extends SecurityManager {

        private final SecurityManager original;

        SystemExitManager() {
            this.original = System.getSecurityManager();
        }

        @Override
        public void checkExit(int status) {
            throw new SecurityException();
        }

        @Override
        public void checkPermission(Permission perm) {
            if (original != null) {
                original.checkPermission(perm);
            }
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            if (original != null) {
                original.checkPermission(perm);
            }
        }

        public SecurityManager getOriginalSecurityManager() {
            return original;
        }
    }
    
    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated KfsAccess object it wraps
     */
    public Object getKfsAccess() {
        return kfsAccess;
    }

    
    public KfsOutputChannel kfs_append_ex(String path, int numReplicas, int mode)
            throws IOException {
        try {
            return new KfsOutputChannel(method_kfs_append_ex.invoke(kfsAccess, path, numReplicas,
                    mode));
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
    
    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive,
            long bufferSize, long readAheadSize) {
        try {
            return new KfsOutputChannel(method_kfs_create.invoke(kfsAccess, path, numReplicas,
                    exclusive, bufferSize, readAheadSize));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean kfs_isDirectory(String path) {
        try {
            return (Boolean) method_kfs_isDirectory.invoke(kfsAccess, path);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int kfs_mkdirs(String path) {
        try {
            return (Integer) method_kfs_mkdirs.invoke(kfsAccess, path);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int kfs_rmdirs(String path) {
        try {
            return (Integer) method_kfs_rmdirs.invoke(kfsAccess, path);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int kfs_remove(String path) {
        try {
            return (Integer) method_kfs_remove.invoke(kfsAccess, path);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int kfs_rename(String oldpath, String newpath) {
        try {
            return (Integer) method_kfs_rename.invoke(kfsAccess, oldpath, newpath);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int kfs_setModificationTime(String path, long time) {
        try {
            return (Integer) method_kfs_setModificationTime.invoke(kfsAccess, path, time);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int kfs_stat(String path, KfsFileAttr attr) {
        try {
            return (Integer) method_kfs_stat.invoke(kfsAccess, path, attr.getKfsFileAttr());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public KfsFileAttr[] kfs_readdirplus(String path) throws IOException {
        try {
            Object[] objs = (Object[]) method_kfs_readdirplus.invoke(kfsAccess, path);
            if (objs == null) {
                throw new IOException("Can't read location");
            }
            KfsFileAttr[] result = new KfsFileAttr[objs.length];
            for (int i = 0; i != objs.length; i++) {
                result[i] = new KfsFileAttr(objs[i]);
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
    
    public void kfs_retToIOException(int ret, String path) throws IOException {
        try {
            method_kfs_retToIOException.invoke(kfsAccess, ret, path);
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
    
    public int kfs_chmod(String path, int mode) {
        try {
            return (Integer) method_kfs_chmod.invoke(kfsAccess, path, mode);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public KfsInputChannel kfs_open(String path) {
        try {
            return new KfsInputChannel(method_kfs_open.invoke(kfsAccess, path));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public KfsFileAttr getFileAttributes(String path) throws IOException {
        KfsFileAttr result = new KfsFileAttr();
        kfs_retToIOException(kfs_stat(path, result), path);
        return result;
    }
    
    private static void checkServer(String host, int port) throws IOException{
        SocketAddress sockaddr = new InetSocketAddress(host, port);
        Socket socket = new Socket();
        try {
            socket.connect(sockaddr, 2000);
        }
        catch (IOException e) {
            throw new IOException("Can't reach metaserver!", e);
        }
        finally {
            try {
                socket.close();
            }
            catch (IOException e) {
                LOGGER.warn("Couldn't close QFS checkServer socket!", e);
            }
        }
    }
}

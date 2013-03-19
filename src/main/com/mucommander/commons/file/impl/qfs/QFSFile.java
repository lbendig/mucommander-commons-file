package com.mucommander.commons.file.impl.qfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mucommander.commons.file.AbstractFile;
import com.mucommander.commons.file.AuthException;
import com.mucommander.commons.file.FileFactory;
import com.mucommander.commons.file.FileOperation;
import com.mucommander.commons.file.FilePermissions;
import com.mucommander.commons.file.FileURL;
import com.mucommander.commons.file.PermissionBits;
import com.mucommander.commons.file.ProtocolFile;
import com.mucommander.commons.file.SimpleFilePermissions;
import com.mucommander.commons.file.SyncedFileAttributes;
import com.mucommander.commons.file.UnsupportedFileOperationException;
import com.mucommander.commons.file.filter.FilenameFilter;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsAccess;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsFileAttr;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsInputChannel;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsOutputChannel;
import com.mucommander.commons.io.ByteCounter;
import com.mucommander.commons.io.ByteUtils;
import com.mucommander.commons.io.CounterOutputStream;
import com.mucommander.commons.io.RandomAccessInputStream;
import com.mucommander.commons.io.RandomAccessOutputStream;

/**
 * Implementation of Quantcast file system (QFS) protocol.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 */
public class QFSFile extends ProtocolFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(QFSFile.class);
    
    /** Marker of current directory */
    private static final String DOT = ".";
    /** Marker of parent directory */
    private static final String DOTDOT = "..";
    /** Wrapper instance of QFS's JNI accessor */
    private KfsAccess kfsAccess;
    private String path;
    /** Cached parent file instance, null if not created yet or if this file has no parent */
    private AbstractFile parent;
    /** Has the parent file been determined yet? */
    private boolean parentValSet;
    /** True if this file is currently being written */
    private boolean isWriting;
    /** Holds file attributes */
    private QFSFileAttributes fileAttributes;
    
    protected QFSFile(FileURL url) throws IOException {
        this(url, null, null);
    }
    
    protected QFSFile(FileURL url, KfsAccess kfsAccess, KfsFileAttr kfsFileAttr) throws IOException {
        super(url);
        if(kfsAccess == null) {
            try {
                
                String host = (url.getHost() == null) ? "/" : url.getHost();
                int port = (url.getPort() == -1) ? url.getStandardPort() : url.getPort();
                this.kfsAccess = new KfsAccess(host, port);
                
            }
            catch(IOException e) {
                throw e;
            }
            catch(Exception e) {
                throw new IOException("QFS init error!", e);
            }
        }
        else {
            this.kfsAccess = kfsAccess;
        }
        this.path = fileURL.getPath();

        this.fileAttributes = (kfsFileAttr == null) ? new QFSFileAttributes() : 
            new QFSFileAttributes(kfsFileAttr);
    }
    
    private OutputStream getOutputStream(boolean append) throws IOException {
        OutputStream os = null;
        boolean overwrite = true;
        KfsOutputChannel channel = append ? kfsAccess.kfs_append_ex(path, (int)1, 0666) :
            kfsAccess.kfs_create(path, 1, !overwrite, -1, -1);
        if (channel == null) {
            throw new IOException("Can't write! Write-protected?");
        }
        else {
            os = Channels.newOutputStream((WritableByteChannel)channel.getKfsOutputChannel());
        }
        OutputStream out = new CounterOutputStream(os,
            new ByteCounter() {
                @Override
                public synchronized void add(long nbBytes) {
                    fileAttributes.addToSize(nbBytes);
                    fileAttributes.setDate(System.currentTimeMillis());
                }
            }
        ) {
            @Override
            public void close() throws IOException {
                super.close();
                isWriting = false;
            }
        };

        // Update local attributes
        fileAttributes.setExists(true);
        fileAttributes.setDate(System.currentTimeMillis());
        fileAttributes.setSize(0);

        isWriting = true;

        return out;
    }
    
    /////////////////////////////////
    // AbstractFile implementation //
    /////////////////////////////////
    
    @Override
    public AbstractFile getParent() {
        if (!parentValSet) {
            FileURL parentFileURL = this.fileURL.getParent();
            if (parentFileURL != null)
                parent = FileFactory.getFile(fileURL.getParent());

            parentValSet = true;
        }

        return parent;
    }

    @Override
    public void setParent(AbstractFile parent) {
        this.parent = parent;
        this.parentValSet = true;
    }

    @Override
    public Object getUnderlyingFileObject() {
        return fileAttributes;
    }

    @Override
    public boolean exists() {
        return fileAttributes.exists();
    }

    @Override
    public boolean isDirectory() {
        return fileAttributes.isDirectory();
    }

    @Override
    public boolean isSymlink() {
        // No support for symlinks
        return false;
    }

    @Override
    public long getDate() {
        return fileAttributes.getDate();
    }

    @Override
    public long getSize() {
        return fileAttributes.getSize();
    }

    @Override
    public PermissionBits getChangeablePermissions() {
        return FilePermissions.FULL_PERMISSION_BITS;
    }

    @Override
    public FilePermissions getPermissions() {
        return fileAttributes.getPermissions();
    }

    @Override
    public String getOwner() {
        return fileAttributes.getOwner();
    }

    @Override
    public boolean canGetOwner() {
        return true;
    }

    @Override
    public String getGroup() {
        return fileAttributes.getGroup();
    }

    @Override
    public boolean canGetGroup() {
        return true;
    }

    @Override
    public void mkdir() throws IOException {
        if (exists() || (kfsAccess.kfs_mkdirs(path) != 0))
            throw new IOException();
        // Update local attributes
        fileAttributes.setExists(true);
        fileAttributes.setDirectory(true);
        fileAttributes.setDate(System.currentTimeMillis());
        fileAttributes.setSize(0);
    }
    
    @Override
    public void delete() throws IOException, UnsupportedFileOperationException {
        int isSuccesful = -1;
        isSuccesful = (kfsAccess.kfs_isDirectory(path)) ? kfsAccess.kfs_rmdirs(path) : 
            kfsAccess.kfs_remove(path);
        if (isSuccesful != 0) {
            throw new IOException();
        }
        // Update local attributes
        fileAttributes.setExists(false);
        fileAttributes.setDirectory(false);
        fileAttributes.setSize(0);
    }

    @Override
    public void renameTo(AbstractFile destFile) throws IOException {
        // Throw an exception if the file cannot be renamed to the specified
        // destination
        checkRenamePrerequisites(destFile, false, false);

        // Delete the destination if it already exists
        if (destFile.exists()) {
            destFile.delete();
        }

        if ((kfsAccess.kfs_rename(path, ((QFSFile) destFile).path)) != 0) {
            throw new IOException();
        }

        // Update destination file attributes by fetching them from the server
        ((QFSFileAttributes) destFile.getUnderlyingFileObject()).fetchAttributes();

        // Update this file's attributes locally
        fileAttributes.setExists(false);
        fileAttributes.setDirectory(false);
        fileAttributes.setSize(0);

    }
    
    @Override
    public void changeDate(long lastModified) throws IOException, UnsupportedFileOperationException {
        kfsAccess.kfs_setModificationTime(path, lastModified);
        // Update local attributes
        fileAttributes.setDate(lastModified);
    }

    @Override
    public void changePermission(int access, int permission, boolean enabled) throws IOException {
        changePermissions(ByteUtils.setBit(getPermissions().getIntValue(),
                (permission << (access * 3)), enabled));
    }

    @Override
    public InputStream getInputStream() throws IOException, UnsupportedFileOperationException {
        return Channels.newInputStream((ReadableByteChannel) kfsAccess.kfs_open(path)
                .getKfsInputChannel());
    }

    @Override
    public OutputStream getOutputStream() throws IOException, UnsupportedFileOperationException {
        return getOutputStream(false);
    }

    @Override
    public RandomAccessInputStream getRandomAccessInputStream() throws IOException {
        return new QFSRandomAccessInputStream(kfsAccess, path, getSize());
    }

    @Override
    public AbstractFile[] ls() throws IOException, UnsupportedFileOperationException {
        // return ls(new StartsWithFilenameFilter("..", false, true));
        return ls(null);
    }

    @Override
    public OutputStream getAppendOutputStream() throws IOException,
            UnsupportedFileOperationException {
        throw new UnsupportedFileOperationException(FileOperation.APPEND_FILE);
    }

    @Override
    public RandomAccessOutputStream getRandomAccessOutputStream() throws IOException,
            UnsupportedFileOperationException {
        throw new UnsupportedFileOperationException(FileOperation.RANDOM_WRITE_FILE);
    }

    @Override
    public void copyRemotelyTo(AbstractFile destFile) throws IOException,
            UnsupportedFileOperationException {
        throw new UnsupportedFileOperationException(FileOperation.COPY_REMOTELY);
    }

    @Override
    public long getFreeSpace() throws IOException, UnsupportedFileOperationException {
        throw new UnsupportedFileOperationException(FileOperation.GET_FREE_SPACE);
    }

    @Override
    public long getTotalSpace() throws IOException, UnsupportedFileOperationException {
        throw new UnsupportedFileOperationException(FileOperation.GET_TOTAL_SPACE);
    }
    
    ////////////////////////
    // Overridden methods //
    ////////////////////////

    @Override
    public AbstractFile[] ls(final FilenameFilter filter) throws IOException {

        // We need to ensure that the file is a directory
        if (!exists() || !isDirectory()) {
            throw new IOException();
        }

        KfsFileAttr[] statuses;
        KfsFileAttr singleAttr = new KfsFileAttr();
        kfsAccess.kfs_stat(path, singleAttr);
        if (singleAttr.getIsDirectory()) {
            statuses = kfsAccess.kfs_readdirplus(path);
        }
        else {
            statuses = new KfsFileAttr[] { singleAttr };
        }

        int nbChildren = (statuses == null) ? 0 : statuses.length;
        List<AbstractFile> children = new ArrayList<AbstractFile>();
        String parentPath = fileURL.getPath();
        if (!parentPath.endsWith("/")) {
            parentPath += "/";
        }
        FileURL childURL;
        KfsFileAttr childStatus;

        for (int i = 0; i < nbChildren; i++) {
            childStatus = statuses[i];
            String filename = childStatus.getFilename();
            if (DOT.equals(filename) || DOTDOT.equals(filename)) {
                continue;
            }
            childURL = (FileURL) fileURL.clone();
            childURL.setPath(parentPath + filename);
            children.add(FileFactory.getFile(childURL, this, kfsAccess, childStatus));
        }

        return children.toArray(new AbstractFile[children.size()]);

    }
    
    @Override
    public void changePermissions(int permissions) throws IOException,
            UnsupportedFileOperationException {
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_chmod(path, permissions), path);
        fileAttributes.setPermissions(new SimpleFilePermissions(permissions));
    }
    
    class QFSFileAttributes extends SyncedFileAttributes {

        private static final int TTL = 60000;

        // this constructor is called by the public constructor
        private QFSFileAttributes() throws AuthException {
            super(TTL, false); // no initial update

            fetchAttributes(); // throws AuthException if no or bad credentials
            updateExpirationDate(); // declare the attributes as 'fresh'
        }

        // this constructor is called by #ls()
        private QFSFileAttributes(KfsFileAttr kfsFileAttr) {
            super(TTL, false); // no initial update

            setAttributes(kfsFileAttr);
            setExists(true);

            updateExpirationDate(); // declare the attributes as 'fresh'
        }

        private void fetchAttributes() throws AuthException {
            // Do not update attributes while the file is being written, as they
            // are not reflected immediately on the
            // name node.
            if (isWriting)
                return;

            try {
                setAttributes(kfsAccess.getFileAttributes(path));
                setExists(true);
            }
            catch (IOException e) {
                // File doesn't exist on the server
                setExists(false);
                setDefaultFileAttributes(getURL(), this);

                // Rethrow AuthException
                if (e instanceof AuthException)
                    throw (AuthException) e;
            }
        }

        /**
         * Sets file attributes
         * 
         * @param kfsFileAttr - KfsFileAttr instance that contains the file attributes
         */
        private void setAttributes(KfsFileAttr kfsFileAttr) {
            boolean isDirectory = kfsFileAttr.getIsDirectory();
            setDirectory(isDirectory);
            setDate(kfsFileAttr.getModificationTime());
            setSize((isDirectory ? 0L : kfsFileAttr.getFilesize()));
            setPermissions(new SimpleFilePermissions(kfsFileAttr.getMode()
                    & PermissionBits.FULL_PERMISSION_INT));
            setOwner(kfsFileAttr.getOwnerName());
            setGroup(kfsFileAttr.getGroupName());
        }
        
        protected void setDefaultFileAttributes(FileURL url, QFSFileAttributes atts) {
            setOwner("");
            setGroup("");
            setPermissions(new SimpleFilePermissions(0664));
        }

        /**
         * Increments the size attribute's value by the given number of bytes.
         * @param increment number of bytes to add to the current size attribute's value
         * 
         */
        private void addToSize(long increment) {
            setSize(getSize() + increment);
        }


        /////////////////////////////////////////
        // SyncedFileAttributes implementation //
        /////////////////////////////////////////

        @Override
        public void updateAttributes() {
            try {
                fetchAttributes();
            }
            catch (Exception e) { // AuthException
                LOGGER.info("Failed to update attributes", e);
            }
        }
    }
    
    private static class QFSRandomAccessInputStream extends RandomAccessInputStream {

        private final KfsInputChannel in;
        private final long length;

        private QFSRandomAccessInputStream(KfsAccess kfsAccess, String path, long length) {
            this.in = kfsAccess.kfs_open(path);
            // this.length = kfsAccess.kfs_filesize(path);
            this.length = length;
        }

        public long getOffset() throws IOException {
            if (in == null) {
                throw new IOException("File closed");
            }
            return in.tell();
        }

        public long getLength() throws IOException {
            return length;
        }

        public void seek(long offset) throws IOException {
            in.seek(offset);
        }

        @Override
        public synchronized int read() throws IOException {
            byte b[] = new byte[1];
            int res = read(b, 0, 1);
            if (res == 1) {
                return ((int) (b[0] & 0xff));
            }
            return -1;
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            final int res = in.read(ByteBuffer.wrap(b, off, len));
            // Use -1 to signify EOF
            if (res == 0) {
                return -1;
            }
            return res;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

}

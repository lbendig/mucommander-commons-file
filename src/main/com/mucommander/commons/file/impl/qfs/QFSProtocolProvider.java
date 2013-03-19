package com.mucommander.commons.file.impl.qfs;

import java.io.IOException;

import com.mucommander.commons.file.AbstractFile;
import com.mucommander.commons.file.FileURL;
import com.mucommander.commons.file.ProtocolProvider;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsAccess;
import com.mucommander.commons.file.impl.qfs.wrapper.KfsFileAttr;

/**
 * A file protocol provider for the Quantcast QFS filesystem.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public class QFSProtocolProvider implements ProtocolProvider {

    public AbstractFile getFile(FileURL url, Object... instantiationParams) throws IOException {
        AbstractFile result = null;
        try {
            if (instantiationParams.length == 0) {
                result = new QFSFile(url);
            }
            else {
                result = new QFSFile(url, (KfsAccess) instantiationParams[0],
                        (KfsFileAttr) instantiationParams[1]);
            }
        }
        catch (LinkageError e) {
            throw new IOException("QFS initialization error!", e);
        }
        return result;
    }
}

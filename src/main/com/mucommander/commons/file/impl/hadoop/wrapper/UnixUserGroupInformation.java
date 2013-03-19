package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.lang.reflect.Method;

import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reflection-based wrapper for org.apache.hadoop.security.UnixUserGroupInformation
 * Note: Only legacy Hadoop versions use this class
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class UnixUserGroupInformation {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnixUserGroupInformation.class);

    public static final String UGI_PROPERTY_NAME = "hadoop.job.ugi";
    
    private static final Class clazz;
    private static final Method method_login;
    private static final Method method_getUserName;
    
    private final Object unixUserGroupInformation;
    
    static {
        Class oClazz = null;
        Method oMethod_login = null;
        Method oMethod_getUserName = null;
        String name = "org.apache.hadoop.security.UnixUserGroupInformation";
        try {
            oClazz = loadHadoopClass(name);
        }
        catch (Exception e) {
            //Not necessarily an error: only legacy Hadoop versions have this class
            LOGGER.info("Legacy class not found: " + name);
        }
        clazz = oClazz;
        try {
            oMethod_login = (oClazz == null) ? null : 
                getMethod(oClazz, "login", Configuration.getClassToken());
            
            oMethod_getUserName = (oClazz == null) ? null : getMethod(oClazz, "getUserName");
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize UnixUserGroupInformation wrapper!", e);
        }
        method_login = oMethod_login;
        method_getUserName = oMethod_getUserName;
        
    }
    
    UnixUserGroupInformation(Object unixUserGroupInformation) {
        this.unixUserGroupInformation = unixUserGroupInformation;
    }
        
    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated UnixUserGroupInformation object it wraps
     */
    public Object getUnixUserGroupInformation() {
        return unixUserGroupInformation;
    }

    public static UnixUserGroupInformation login(Configuration conf) throws LoginException {
        try {
            return new UnixUserGroupInformation(method_login.invoke(null, conf.getConfiguration()));
        }
        catch (Exception e) {
            if (e.getCause() instanceof LoginException) {
                throw (LoginException)e.getCause();
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Return the user's name
     */
    public String getUserName() {
        try {
            return (String) method_getUserName.invoke(unixUserGroupInformation);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}

package com.mucommander.commons.file.impl.hadoop.wrapper;

import static com.mucommander.commons.file.util.ClassLoaderUtils.getMethod;
import static com.mucommander.commons.file.util.ClassLoaderUtils.loadHadoopClass;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Reflection-based wrapper for org.apache.hadoop.security.UserGroupInformation
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class UserGroupInformation {

    private static final Class clazz;
    private static final Method method_getCurrentUser;
    private static final Method method_getShortUserName;

    private final Object userGroupInformation;
    
    static {
        try {
            clazz = loadHadoopClass("org.apache.hadoop.security.UserGroupInformation");
            method_getCurrentUser = getMethod(clazz, "getCurrentUser");
            method_getShortUserName = getMethod(clazz, "getShortUserName");
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Can't initialize UserGroupInformation wrapper!", e);
        }
        
    }
    
    UserGroupInformation(Object unixUserGroupInformation) {
        this.userGroupInformation = unixUserGroupInformation;
    }
        
    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated UserGroupInformation object it wraps
     */
    public Object getUserGroupInformation() {
        return userGroupInformation;
    }
    
    /**
     * Return the current user, including any doAs in the current stack.
     * @return the current user
     * @throws IOException if login fails
     */
    public static UserGroupInformation getCurrentUser() throws IOException {
        try {
            return new UserGroupInformation(method_getCurrentUser.invoke(null));
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
     * Get the user's login name.
     * @return the user's name up to the first '/' or '@'.
     */
    public String getShortUserName() {
        try {
            return (String) method_getShortUserName.invoke(userGroupInformation);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    
}

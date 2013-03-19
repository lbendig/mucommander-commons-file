package com.mucommander.commons.file.impl.qfs.wrapper;

/**
 * Reflection-based wrapper for com.quantcast.qfs.access.KfsOutputChannel.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
@SuppressWarnings("rawtypes")
public class KfsOutputChannel {

    private static final Class clazz = null;
    private final Object kfsOutputChannel;

    KfsOutputChannel(Object kfsOutputChannel) {
        this.kfsOutputChannel = kfsOutputChannel;
    }

    public static Class getClassToken() {
        return clazz;
    }

    /**
     * @return The instantiated KfsOutputChannel object it wraps
     */
    public Object getKfsOutputChannel() {
        return kfsOutputChannel;
    }

}

package com.alibaba.jstorm.metric;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public interface KVSerializable {
    String START = "S", END = "E";
    int LONG_SIZE = 8;
    int INT_SIZE = 4;

    byte[] getKey();

    byte[] getValue();

    Object fromKV(byte[] key, byte[] value);
}

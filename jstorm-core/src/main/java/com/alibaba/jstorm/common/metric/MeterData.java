package com.alibaba.jstorm.common.metric;


import com.alibaba.jstorm.metric.Bytes;
import com.alibaba.jstorm.metric.KVSerializable;

/**
 * @author wange
 * @since 15/6/23
 */
public class MeterData extends MetricBaseData implements KVSerializable {
    private double m1;

    public double getM1() {
        return m1;
    }

    public void setM1(double m1) {
        this.m1 = m1;
    }

    @Override
    public byte[] getValue() {
        byte[] ret = new byte[8];
        Bytes.putDouble(ret, 0, m1);

        return ret;
    }

    @Override
    public Object fromKV(byte[] key, byte[] value) {
        parseKey(key);
        this.m1 = Bytes.toDouble(value, 0);

        return this;
    }
}

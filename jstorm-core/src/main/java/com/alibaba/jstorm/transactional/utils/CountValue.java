package com.alibaba.jstorm.transactional.utils;

public class CountValue {
    public int count = 0;

    public CountValue() {
    }

    public int getValue() {
        return count;
    }

    public int getValueAndSet(int value) {
        int ret = count;
        count = value;
        return ret;
    }

    public void setValue(int value) {
        count = value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CountValue && ((CountValue) obj).count == count;
    }

    @Override
    public String toString() {
        return String.valueOf(count);
    }
}
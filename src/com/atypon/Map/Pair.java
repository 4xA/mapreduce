package com.atypon.Map;

import java.io.Serializable;
import java.util.Arrays;

public class Pair implements Serializable {
    private String key;
    private Object[] value;

    public Pair(String key, Object[] value) {
        this.key = key;
        this.value = value.clone();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object[] value) {
        this.value = value;
    }

    public String toString() {
        return String.format("{%s, %s}", key, Arrays.toString(value));
    }
}

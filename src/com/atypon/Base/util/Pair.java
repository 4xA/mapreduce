package com.atypon.Base.util;

import com.atypon.Map.Mapper;

import java.io.Serializable;
import java.util.Arrays;

/**
 * {@link Pair} represents data both in the {@link Mapper#map(String)}
 * operation and the {@link com.atypon.Reduce.Reducer#reduce(String, Object[])}
 * operation.
 * @author Asa Abbad
 */
public class Pair implements Serializable, Comparable<Pair>{
    private String key;
    private Object value;

    /**
     * Instantiate with string-value pair
     * @param key
     * @param value
     */
    public Pair(String key, Object value) {
        this.key = key;

        // TODO: validate if this is correct
        if (isArray(value))
            this.value = ((Object[]) value).clone();
        else
            this.value = value;
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

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        // Format "value" as array if it is
        if (isArray(value))
            return String.format("%s,%s", key, Arrays.toString((Object[]) value));

        return String.format("%s,%s", key, value);
    }

    @Override
    public int compareTo(Pair other) {
        if (this.key == null) return -Integer.MAX_VALUE;
        return this.key.compareTo(other.key);
    }

    private boolean isArray(Object o) {
        return o != null && o.getClass().isArray();
    }
}

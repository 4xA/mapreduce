package com.atypon.Reduce;

import com.atypon.Map.Pair;

public class Reducer {
    private Reducer() {}

    public static Pair reduce(String key, Object[] values) {
        int sum = 0;

        for (Object v : values) {
            sum += (int) v;
        }

        return new Pair(key, sum);
    }
}

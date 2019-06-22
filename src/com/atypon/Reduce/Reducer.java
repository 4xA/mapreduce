package com.atypon.Reduce;

import com.atypon.Base.util.Pair;

/**
 * {@link Reducer} is the class used by the Reduce Server
 * for performing the essential reduce operation.
 *
 * <p>
 *     <b>Note: change the implementation of the reduce method here
 *     before compilation.</b>
 * </p>
 * @author Asa Abbad
 */
public class Reducer {
    private Reducer() {}
    /**
     * Reduce operation
     * <p><b>Change this implementation for different job</b></p>
     * @param key
     * @param values
     * @return
     */
    public static Pair reduce(String key, Object[] values) {
        int sum = 0;

        for (Object v : values) {
            sum += (int) v;
        }

        return new Pair(key, sum);
    }
}

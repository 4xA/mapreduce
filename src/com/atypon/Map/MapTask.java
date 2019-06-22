package com.atypon.Map;

import com.atypon.Base.util.Pair;

/**
 * {@link MapTask} performs the {@link Mapper#map(String)}
 * operation allowing for thread use.
 * @author Asa Abbad
 */
public class MapTask implements Runnable {
    String[] keys;
    Pair[] mapped;

    /**
     * Instantiate a map task
     * @param keys  keys to be mapped
     */
    public MapTask(String[] keys) {
        this.keys = keys;
        mapped = new Pair[keys.length];
    }

    @Override
    public void run() {
        for (int i = 0; i < keys.length; i++)
            mapped[i] = new Pair(keys[i], Mapper.map(keys[i]));
    }

    public Pair[] getMapped() {
        return mapped;
    }
}

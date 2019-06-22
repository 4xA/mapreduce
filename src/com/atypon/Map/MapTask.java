package com.atypon.Map;

public class MapTask implements Runnable {
    String[] keys;
    Pair[] mapped;

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

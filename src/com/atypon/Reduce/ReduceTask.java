package com.atypon.Reduce;

import com.atypon.Map.Pair;

public class ReduceTask implements Runnable {
    private Pair[] pairs;
    private Pair[] reduced;

    public ReduceTask(Pair[] pairs) {
        this.pairs = pairs;
        this.reduced = new Pair[pairs.length];
    }

    @Override
    public void run() {
        Pair p;
        for (int i = 0; i < pairs.length; i++) {
            p = pairs[i];
            reduced[i] = Reducer.reduce(p.getKey(), (Object[]) p.getValue());
        }
    }

    public Pair[] getReduced() {
        return reduced;
    }
}

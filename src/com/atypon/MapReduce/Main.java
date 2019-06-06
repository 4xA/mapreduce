package com.atypon.MapReduce;

import com.atypon.MapReduce.core.Job;
import com.atypon.MapReduce.core.JobConfig;

public class Main {
    public static void main(String[] args) {
        JobConfig config = new JobConfig();

        config.setInputMethod(JobConfig.FILE);
//        config.setFileName("test.txt");
//        config.setSplitters(":", "-", ".", "***");
        config.setFileName("input.txt");
        config.setSplitters(String.format("%n"));
        config.setMapNodesCount(10);

        Job job = new Job(config);
        job.start();
    }
}

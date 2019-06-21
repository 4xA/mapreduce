package com.atypon.MapReduce;

import com.atypon.Globals;
import com.atypon.MapReduce.core.Job;
import com.atypon.MapReduce.core.JobConfig;

public class Main {
    public static void main(String[] args) {
        JobConfig config = new JobConfig();

        config.setInputMethod(JobConfig.FILE);
        config.setFileName(Globals.INPUT_FILE_NAME);
        config.setSplitterRegex("[a-zA-Z]+");
        config.setMapNodesCount(2);
        config.setReduceNodesCount(7);
        config.setMapServerPort(6000);
        config.setReduceServerPort(10000);

        Job job = new Job(config);
        job.start();
    }
}

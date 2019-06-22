package com.atypon.Base;

import com.atypon.Globals;
import com.atypon.Base.core.Job;
import com.atypon.Base.core.JobConfig;

public class Main {
    public static void main(String[] args) {
        JobConfig config = new JobConfig();

        config.setInputMethod(JobConfig.FILE);
        config.setInputFileName(Globals.INPUT_FILE_NAME);
        config.setOutputFileName(Globals.OUTPUT_FILE_NAME);
        config.setPerformanceFileName(Globals.PERFORMANCE_FILE_NAME);
        config.setSplitterRegex("[a-zA-Z]+");
        config.setMapNodesCount(4);
        config.setReduceNodesCount(2);
        config.setMapServerPort(6000);
        config.setReduceServerPort(10000);

        Job job = new Job(config);
        job.start();
    }
}

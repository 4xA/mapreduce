package com.atypon.Base.core;

/**
 * JobConfig is an information class that
 * encapsulates all the configurations and
 * pre-requirements needed by a MapReduce
 * Job.
 * @author Asa Abbad
 */
public class JobConfig {
    public static final int FILE = 0;
    public static final int DEFAULT = FILE;

    private int inputMethod;
    private String inputFileName;
    private String outputFileName;
    private String performanceFileName;
    private String[] splitters;
    private String splitterRegex;
    private int mapNodesCount;
    private int reduceNodesCount;
    private int mapServerPort;
    private int reduceServerPort;

    /**
     * Instantiate a configuration
     */
    public JobConfig() {
        inputMethod = DEFAULT;
        inputFileName = null;
        outputFileName = null;
        splitters = new String[0];
        splitterRegex = "";
        mapNodesCount = 0;
    }

    /**
     * Set method of input
     * @param inputMethod   input type
     */
    public void setInputMethod(int inputMethod) {
        if (inputMethod != FILE)
            throw new IllegalArgumentException("unsupported read method");

        this.inputMethod = inputMethod;
    }

    public void setInputFileName(String inputFileName) {
        if (inputFileName == null)
            throw new IllegalArgumentException();

        this.inputFileName = inputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public void setPerformanceFileName(String performanceFileName) {
        this.performanceFileName = performanceFileName;
    }

    public void setSplitters(String ...splitters) {
        this.splitters = splitters;
    }

    public void setSplitterRegex(String splitterRegex) {
        this.splitterRegex = splitterRegex;
    }

    public void setMapNodesCount(int mapNodesCount) {
        this.mapNodesCount = mapNodesCount;
    }

    public void setReduceNodesCount(int reduceNodesCount) {
        this.reduceNodesCount = reduceNodesCount;
    }

    public void setMapServerPort(int mapServerPort) {
        this.mapServerPort = mapServerPort;
    }

    public void setReduceServerPort(int reduceServerPort) {
        this.reduceServerPort = reduceServerPort;
    }

    public int getInputMethod() {
        return inputMethod;
    }

    public String getInputFileName() {
        return this.inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public String getPerformanceFileName() {
        return performanceFileName;
    }

    public String[] getSplitters() {
        return splitters;
    }

    public String getSplitterRegex() {
        return splitterRegex;
    }

    public int getMapNodesCount() {
        return mapNodesCount;
    }

    public int getReduceNodesCount() {
        return reduceNodesCount;
    }

    public int getMapServerPort() {
        return mapServerPort;
    }

    public int getReduceServerPort() {
        return reduceServerPort;
    }
}

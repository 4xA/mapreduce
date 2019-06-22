package com.atypon.Base.core;

public class JobConfig {
    public static final int FILE = 0;
    public static final int DEFAULT = FILE;

    private int inputMethod;
    private String fileName;
    private String[] splitters;
    private String splitterRegex;
    private int mapNodesCount;
    private int reduceNodesCount;
    private int mapServerPort;
    private int reduceServerPort;

    public JobConfig() {
        inputMethod = DEFAULT;
        fileName = null;
        splitters = new String[0];
        splitterRegex = "";
        mapNodesCount = 0;
    }

    public void setInputMethod(int inputMethod) {
        if (inputMethod != FILE)
            throw new IllegalArgumentException("unsupported read method");

        this.inputMethod = inputMethod;
    }

    public void setFileName(String fileName) {
        if (fileName == null)
            throw new IllegalArgumentException();

        this.fileName = fileName;
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

    public String getFileName() {
        return fileName;
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

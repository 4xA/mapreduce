package com.atypon.MapReduce.core;

public class JobConfig {
    public static final int FILE = 0;
    public static final int DEFAULT = FILE;

    private int inputMethod;
    private String fileName;
    private String[] splitters;
    private int mapNodesCount;

    public JobConfig() {
        inputMethod = DEFAULT;
        fileName = null;
        splitters = new String[0];
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

    public void setMapNodesCount(int mapNodesCount) {
        this.mapNodesCount = mapNodesCount;
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

    public int getMapNodesCount() {
        return mapNodesCount;
    }
}

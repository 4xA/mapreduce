package com.atypon.MapReduce.node;

import java.io.*;

public class Node {
    private Process process;
    private int port;

    public Node(int port) {
        this.port = port;
    }

    // process is not created at object instantiation
    // for performance optimization
    protected void createProcess(Class klass) {
        // TODO: make this environment independent
         String javaBin = File.separator + "usr" +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, String.format("%d", port));

        builder.redirectErrorStream();


        try {
            this.process = builder.start();
        } catch (IOException e) {
            System.out.println("Could not create node");
        }
    }

    public Process getProcess() {
        return process;
    }

    public void stopProcess() {
        this.process.destroyForcibly();
    }

    public int getPort() {
        return port;
    }
}

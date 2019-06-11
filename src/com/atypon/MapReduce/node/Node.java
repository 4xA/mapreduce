package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.MapReduce.io.NodeSocketHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Node {
    private Process process;
    private int port;
    protected NodeSocketHandler socketHandler;

    public Node(int port) {
        this.port = port;
    }

    // process is not created at object instantiation
    // for performance optimization
    protected void createProcess(Class klass) throws IOException {
        String binPath = System.getProperty("java.home");

        String javaBin = binPath +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, String.format("%d", port));

        builder.redirectErrorStream();

        this.process = builder.start();

        this.socketHandler = new NodeSocketHandler(this);
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

    // Server operations
    public <T> void send(List<T> l) {
        socketHandler.writeToProcess(l);
        sendSignal(Globals.EOI_MSG);
    }

    public void sendSignal(String signal) {
        socketHandler.writeToProcess(
                 new ArrayList<String>() {{ add(signal); }}
         );
    }

    public ArrayList<String> receive(int max) {
        return socketHandler.readFromProcess(max);
    }

    public void destroy() {
        socketHandler.writeToProcess(
                new ArrayList<String>() {{ add(Globals.END_MSG); }}
        );

        socketHandler.end();
    }

    // End of server operations
}

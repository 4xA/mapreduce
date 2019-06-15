package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.MapReduce.io.NodeSocketHandler;

import java.io.*;

public class Node {
    private Process process;
    private int port;
    private NodeSocketHandler socketHandler;

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

    public int getPort() {
        return port;
    }

    public NodeSocketHandler getNodeSocketHandler() {
        return socketHandler;
    }

    public void setPort(int port) {
        this.port = port;
    }

    // Server operations
    public void sendText(String[] l) {
        socketHandler.writeTextToProcess(l);
        sendSignal(Globals.EOI_MSG);
    }

    public void send(Object object) {
        socketHandler.writeToProcess(object);
    }

    public void sendSignal(String signal) {
        socketHandler.writeTextToProcess(
                new String[] { signal }
         );
    }

    public Object receive() {
        return socketHandler.readObjectFromProcess();
    }

    public void destroy() {
        socketHandler.end();
        this.process.destroyForcibly();
    }
    // End of server operations
}

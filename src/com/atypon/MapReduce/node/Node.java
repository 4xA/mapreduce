package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.MapReduce.io.NodeSocketHandler;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class Node {
    private Process process;
    private int port;
    private NodeSocketHandler socketHandler;
    private long startTime;
    private long endTime;
    private long heapMemoryUsed;

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

    public void createSocketHandler() {
        // TODO: this function must be used carfuly. Review.
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

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getExcecutionTimeFormated() {
        long time = endTime - startTime;

        long seconds = TimeUnit.MILLISECONDS.toSeconds(time);
        long milliseconds = time - TimeUnit.SECONDS.toMillis(seconds);

        return String.format("%d.%d", seconds, milliseconds);
    }

    public long getHeapMemoryUsed() {
        return heapMemoryUsed;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setHeapMemoryUsed(long heapMemoryUsed) {
        this.heapMemoryUsed = heapMemoryUsed;
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

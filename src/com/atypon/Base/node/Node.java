package com.atypon.Base.node;

import com.atypon.Globals;
import com.atypon.Base.io.NodeSocketHandler;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * {@link Node} is a representation of a
 * MapReduce server. Simplifying creation,
 * access and deletion.
 * @author Asa Abbad
 */
public class Node {
    private Process process;
    private int port;
    private NodeSocketHandler socketHandler;
    private long startTime;
    private long endTime;
    private long heapMemoryUsed;

    /**
     * Instantiate {@link Node} class
     * @param port  Port at which server should listen
     */
    public Node(int port) {
        this.port = port;
    }

    // process is not created at object instantiation
    // for performance optimization
    /**
     * Create the server as a process and pass
     * all necessary information to the server.
     * @param klass Class to be created. <b>Note: must contain a main method</b>
     * @throws IOException  if process could not be created by JVM
     */
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

    /**
     * Run server and perform all operations from start to completion
     */
    public void run() {}

    /**
     * Create a {@link NodeSocketHandler} or recreate it.
     * <p>
     *     This method should be used to run the server
     *     manually without relying on {@link Node#run()}.
     * </p>
     */
    public void createSocketHandler() {
        // TODO: this function must be used carefuly. Review.
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

    /**
     * Generate excecution time in the format <b>Seconds.Milliseconds</b>
     * @return  {@link String} representing execution time.
     */
    public String getExcecutionTimeFormated() {
        long time = endTime - startTime;

        long seconds = TimeUnit.MILLISECONDS.toSeconds(time);
        long milliseconds = time - TimeUnit.SECONDS.toMillis(seconds);

        return String.format("%d.%d", seconds, milliseconds);
    }

    /**
     * Get heap memory used in <b>bytes</b>
     * @return Heap memory used in <b>bytes</b>
     */
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

    /**
     * Send array of {@link String} over socket.
     * @param text  Array of {@link String} to be sent
     */
    public void sendText(String[] text) {
        socketHandler.writeTextToProcess(text);
        sendSignal(Globals.EOI_MSG);
    }

    /**
     * Send {@link Object} over socket
     * @param object    {@link Object} to be sent
     */
    public void send(Object object) {
        socketHandler.writeToProcess(object);
    }

    /**
     * Send a command signal to teh socket
     * @param signal    Server command signal
     */
    public void sendSignal(String signal) {
        socketHandler.writeTextToProcess(
                new String[] { signal }
         );
    }

    /**
     * Receive {@link Object} from socket.
     * @return  {@link Object} from server
     */
    public Object receive() {
        return socketHandler.readObjectFromProcess();
    }

    /**
     * Destroying {@link Node} includes releasing
     * held resources and killing process running
     * the server.
     */
    public void destroy() {
        socketHandler.end();
        this.process.destroyForcibly();
    }
    // End of server operations
}

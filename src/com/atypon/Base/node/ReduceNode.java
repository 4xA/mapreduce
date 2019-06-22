package com.atypon.Base.node;

import com.atypon.Globals;
import com.atypon.Base.util.Pair;
import com.atypon.Reduce.ReduceServer;

import java.io.IOException;

/**
 * {@link ReduceNode} is a high-level representation of a
 * Reduce server. {@link ReduceNode} is responsible for an entire
 * life-cycle of a Reduce server from creation to
 * reading/writing and finally to terminating.
 * @author Asa Abbad
 */
public class ReduceNode extends Node {
    Pair[] mappedData;
    Pair[] reducedData;

    /**
     * Instantiate a ReduceNode.
     * <b>The server will not run until
     * it is explicitly told to by calling
     * {@link ReduceNode#run()}</b>.
     * @param port  port at which server will listen
     * @param mappedData    mapped data for the reduce operation
     */
    public ReduceNode(int port, Pair[] mappedData) {
        super(port);
        this.mappedData = mappedData;
    }

    @Override
    public void run() {
        this.setStartTime(System.currentTimeMillis());

        createReduceNodeSafe();

        sendData();

        Object o;
        while (! ( o = this.receiveData() ).equals(Globals.EOF_MSG))
            this.reducedData = (Pair[]) o;

        this.setHeapMemoryUsed((long) this.receiveData());

        this.destroy();
    }

    /**
     * Creates a Reduce Node at the specified port
     * @throws IOException  if port is busy
     */
    public void createReduceNode() throws IOException {
        createProcess(ReduceServer.class);
        try {
            this.getNodeSocketHandler().createObjectInputStream();
            this.getNodeSocketHandler().createObjectOutputStream();
        } catch (IllegalAccessException e) {
            // TODO: handle better
            e.printStackTrace();
        }
    }

    /**
     * Creates a Reduce Node at the specified port.
     * If that port is busy, {@link ReduceNode#createReduceNodeSafe()}
     * will try to find another available port.
     */
    public void createReduceNodeSafe() {
        // TODO: review this solution
        // try other ports if used by other program (Handle IOException)
        for (int port = this.getPort(); port < 65535; port++) {
            try {
                this.createReduceNode();
                System.out.println(String.format("Reduce node created at port: %d", port));
                break;
            } catch (IOException e) {
                System.out.println("Port busy\nTrying new port...");
                this.setPort(port);
                continue;
            }
        }
    }

    /**
     * Send available data to server
     */
    public void sendData() {
        this.send(mappedData);
    }

    /**
     * Receive data from server
     * @return  Reduced data from server
     */
    public Object receiveData() {
        this.send(Globals.SND_MSG);
        return this.receive();
    }

    @Override
    public void destroy() {
        this.send(Globals.END_MSG);
        super.destroy();

        this.setEndTime(System.currentTimeMillis());
    }

    public Pair[] getMappedData() { return mappedData; }

    public Pair[] getReducedData() {
        return reducedData;
    }

    @Override
    public String toString() {
        return String.format("{ReduceNode: Port=%d}", this.getPort());
    }
}

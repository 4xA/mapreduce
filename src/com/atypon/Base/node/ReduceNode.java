package com.atypon.Base.node;

import com.atypon.Globals;
import com.atypon.Map.Pair;
import com.atypon.Reduce.ReduceServer;

import java.io.IOException;

public class ReduceNode extends Node {
    Pair[] mappedData;
    Pair[] reducedData;

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

    public void createReduceNode() throws IOException {
        createProcess(ReduceServer.class);
        this.getNodeSocketHandler().createObjectInputStream();
        this.getNodeSocketHandler().createObjectOutputStream();
    }

    private void createReduceNodeSafe() {
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

    private void sendData() {
        this.send(mappedData);
    }

    private Object receiveData() {
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

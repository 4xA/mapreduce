package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.Map.Pair;
import com.atypon.Reduce.ReduceServer;

import java.io.IOException;
import java.util.Arrays;

public class ReduceNode extends Node implements java.io.Serializable {
    Pair[] mappedData;

    public ReduceNode(int port, Pair[] mappedData) {
        super(port);
        this.mappedData = mappedData;
    }

    public void run() {
        createReduceNodeSafe();

        sendData();

        Object o;
        while (! ( o = receiveData() ).equals(Globals.EOF_MSG))
            System.out.println(Arrays.toString((Object[]) o));
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
    }

    @Override
    public String toString() {
        return String.format("{ReduceNode: Port=%d}", this.getPort());
    }
}

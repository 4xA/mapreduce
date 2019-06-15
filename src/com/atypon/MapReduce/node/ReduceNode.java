package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.Map.Pair;
import com.atypon.Reduce.ReduceServer;

import java.io.IOException;
import java.util.ArrayList;

public class ReduceNode extends Node implements java.io.Serializable {
    public ReduceNode(int port) {
        super(port);
    }

    public void run() {
        createReduceNodeSafe();

        sendData();
        System.out.println(receiveData());
        System.out.println(receiveData());
        System.out.println(receiveData());
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
        ArrayList<String> list = new ArrayList<String>() {{
            add("let");
            add("me");
            add("tel");
        }};

        this.send(new Pair("Hello", list.toArray()));
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
}

package com.atypon.MapReduce.node;

import com.atypon.Globals;
import com.atypon.Map.MapperServer;

import java.io.IOException;

public class MapNode extends Node implements java.io.Serializable {
    private String[] data;

    public MapNode(int port, String[] data) {
        super(port);
        this.data = data;
    }

    public void run() {
        createMapNodeSafe();
        sendData();
        System.out.println(this.receive());
    }

    public void createMapNode() throws IOException {
        createProcess(MapperServer.class);
        this.getNodeSocketHandler().createStringOutputStream();
        this.getNodeSocketHandler().createObjectInputStream();
    }

    private void createMapNodeSafe() {
        // TODO: review this solution
        // try other ports if used by other program (Handle IOException)
        for (int port = this.getPort(); port < 65535; port++) {
            try {
                this.createMapNode();
                System.out.println(String.format("Map node created at port: %d", port));
                break;
            } catch (IOException e) {
                System.out.println("Port busy\nTrying new port...");
                this.setPort(port);
                continue;
            }
        }
    }

    private void sendData() {
        sendText(data);
    }

    @Override
    public void destroy() {
        sendText(new String[] { Globals.END_MSG });
        super.destroy();
    }
}

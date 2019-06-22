package com.atypon.Base.node;

import com.atypon.Globals;
import com.atypon.Map.MapperServer;
import com.atypon.Map.Pair;

import java.io.IOException;

public class MapNode extends Node {
    private String[] data;
    private Pair[] mappedData;
    private int[] ports;

    public MapNode(int port, String[] data) {
        super(port);
        this.data = data;
    }

    @Override
    public void run() {
        createMapNodeSafe();
        sendData();

        this.mappedData = (Pair[]) this.receive();
        this.setHeapMemoryUsed((long) this.receive());
        this.destroy();
    }

    public void createMapNode() throws IOException {
        this.setStartTime(System.currentTimeMillis());

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

    private void sendPorts() {
        sendSignal(Globals.PRT_MSG);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < ports.length-1; i++) {
            sb.append(ports[i]).append(",");
        }
        sb.append(ports[ports.length-1]);

        sendText(new String[] { sb.toString() });
    }

    private void sendData() {
        sendText(data);
    }

    @Override
    public void destroy() {
        sendText(new String[] { Globals.END_MSG });
        super.destroy();

        this.setEndTime(System.currentTimeMillis());
    }

    public String[] getData() {
        return data;
    }

    public Pair[] getMappedData() {
        return mappedData;
    }
}

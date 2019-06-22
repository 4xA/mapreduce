package com.atypon.Base.node;

import com.atypon.Globals;
import com.atypon.Map.MapperServer;
import com.atypon.Base.util.Pair;

import java.io.IOException;

/**
 * {@link MapNode} is a high-level representation of a
 * Map server. {@link MapNode} is responsible for an entire
 * life-cycle of a Map server from creation to
 * reading/writing and finally to terminating.
 * @author Asa Abbad
 */
public class MapNode extends Node {
    private String[] data;
    private Pair[] mappedData;
    private int[] ports;

    /**
     * Instantiate a MapNode.
     * <b>The server will not run until
     * it is explicitly told to by calling
     * {@link MapNode#run()}</b>.
     * @param port  port at which server will listen
     * @param data  {@link String}[] data to be sent for mapping
     */
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

    /**
     * Creates a Map Node at the specified port
     * @throws IOException  if port is busy
     */
    public void createMapNode() throws IOException {
        this.setStartTime(System.currentTimeMillis());

        createProcess(MapperServer.class);
        try {
            this.getNodeSocketHandler().createStringOutputStream();
            this.getNodeSocketHandler().createObjectInputStream();
        } catch (IllegalAccessException e) {
            // TODO: handle better
            e.printStackTrace();
        }
    }

    /**
     * Creates a Map Node at the specified port.
     * If that port is busy, {@link MapNode#createMapNodeSafe()}
     * will try to find another available port.
     */
    public void createMapNodeSafe() {
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

    /**
     * Send available data to server
     */
    public void sendData() {
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

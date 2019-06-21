package com.atypon.Map;

import com.atypon.Globals;
import com.atypon.MapReduce.node.ReduceNode;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class MapperServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedReader in;
    private ObjectOutputStream objOut;
    private ReduceNode[] reduceNodes;
    private int numReduceNodes;
    private String[] portStrings;

    private Pair[] mappedData;

    public void start(int port) throws IOException {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new IOException(port + "");
        }
        clientSocket = serverSocket.accept();

        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        objOut = new ObjectOutputStream(clientSocket.getOutputStream());

        // Mapping
        read();

        // connect
//        connectToReduceNodes(portStrings);
//        objOut.writeObject(new Pair(Globals.PRT_MSG, Arrays.toString(reduceNodes))); // TODO: remove: only for testing

        // Sorting
        sort(mappedData);

        // Print Output to main process
        printOutput();

        // Send data to Reduce Nodes
        sendToReduceNodes();

        // Stop Server
        stop();
    }

    public void stop() throws IOException {
        in.close();
        objOut.close();
        clientSocket.close();
        serverSocket.close();
    }

    private void read() throws IOException {
        Object value;

        ArrayList<Pair> list = new ArrayList<Pair>();

        String inputLine;
        while ((inputLine = in.readLine()) != null) {

            // STOP SIGNAL
            if (inputLine.equals(Globals.EOI_MSG)) {
                break;
            }

            // Receive NumReduceNodes
            if (inputLine.equals(Globals.NRN_MSG)) {
                this.numReduceNodes = Integer.parseInt(in.readLine());
                objOut.writeObject(new Pair(Globals.NRN_MSG, numReduceNodes));

                // Ignore EOI signal
                in.readLine();
                continue;
            }

            // Receive ports SIGNAL
            if (inputLine.equals(Globals.PRT_MSG)) {
                portStrings = in.readLine().split(",");
                // Ignore EOI signal
                in.readLine();
                continue;
            }

            value = Mapper.map(inputLine);
            list.add(new Pair(inputLine, value));
        }

        mappedData = list.toArray(new Pair[0]);
    }

    private static void sort(Pair[] pairs) {
        Arrays.sort(pairs);
    }

    private void printOutput() throws IOException {
        objOut.writeObject(mappedData);
    }

    private void connectToReduceNodes(String[] ports) {
        reduceNodes = new ReduceNode[ports.length];

        for (int i = 0; i < reduceNodes.length; i++) {
//            reduceNodes[i] = new ReduceNode(Integer.parseInt(ports[i]));
//            reduceNodes[i].createSocketHandler();
        }
    }

    private void sendToReduceNodes() {
        int reduceNumber;
        for (Pair p : mappedData) {
            reduceNumber = (p.getKey().hashCode() & Integer.MAX_VALUE) % numReduceNodes;

            reduceNodes[reduceNumber].send(p);
        }
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        try {
            new MapperServer().start(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

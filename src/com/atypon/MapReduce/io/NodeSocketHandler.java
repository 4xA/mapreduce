package com.atypon.MapReduce.io;

import com.atypon.MapReduce.node.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;

public class NodeSocketHandler {
    private Node node;
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    public NodeSocketHandler(Node node) {
        if (node == null)
            throw new IllegalArgumentException("Node passed to IO handler is null");

        this.node = node;
        port = node.getPort();
        try {
            while (true) {
                try {
                    clientSocket = new Socket("127.0.0.1", port);
                    break;
                } catch (ConnectException e) {
                    continue;
                }
            }

            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            System.out.println("Could not establish server connection at port: " + port);
        }
    }

    public NodeSocketHandler writeToProcess(Iterable text) {
        // TODO: should use thread?
        // TODO: throttle input so not to overload heap
        for (Object o : text)
            out.println(o.toString());

        return this;
    }

    // throttle is used as a balance between the advantage of
    // buffered read while not overloading memory
    public ArrayList<String> readFromProcess(int throttle) {
        // TODO: should use thread?
        // TODO: test throttle

        ArrayList<String> list = new ArrayList<String>();

        try {
            String s;
            // TODO: make "END" env variable
            while (throttle-- > 0 && (s = in.readLine()) != null && !s.equals("END"))
                list.add(s);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.out.println("IOException reading from node");
        }

        return list;
    }

    public void end() {
        try {
            clientSocket.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        node.stopProcess();
    }
}

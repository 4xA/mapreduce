package com.atypon.MapReduce.io;

import com.atypon.MapReduce.node.Node;

import java.io.*;
import java.util.ArrayList;

public class NodeIOHandler {
    private Node node;
    private Process process;
    private BufferedWriter writer;
    private BufferedReader reader;

    public NodeIOHandler(Node node) {
        if (node == null)
            throw new IllegalArgumentException("Node passed to IO handler is null");

        this.node = node;
        process = node.getProcess();

        // write streams
        OutputStream stdin = process.getOutputStream();
        writer = new BufferedWriter(new OutputStreamWriter(stdin));

        // read streams
        InputStream stdout = process.getInputStream();
        reader = new BufferedReader(new InputStreamReader(stdout));
    }

    public NodeIOHandler writeToProcess(Iterable text) {
        // TODO: should use thread?
        // TODO: throttle input so not to overload heap
        try {
            for (Object o : text)
                writer.write(o.toString());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.out.println("IOException writing to node");
        }

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
            while (throttle-- > 0 && (s = reader.readLine()) != null)
                list.add(s);
        } catch (IOException e) {
            System.out.println("IOException reading from node");
        }

        return list;
    }
}

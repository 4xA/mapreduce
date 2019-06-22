package com.atypon.Base.io;

import com.atypon.Globals;
import com.atypon.Base.node.Node;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;

public class NodeSocketHandler {
    private Node node;
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private ObjectInputStream objIn;
    private ObjectOutputStream objOut;

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
        } catch (IOException e) {
            System.out.println("Could not establish server connection at port: " + port);
        }
    }

    public NodeSocketHandler writeTextToProcess(String[] text) {
        // TODO: should use thread?
        // TODO: throttle input so not to overload heap
        for (String s : text)
            out.println(s);
        out.flush();

        return this;
    }

    public NodeSocketHandler writeToProcess(Object o) {
        try {
            objOut.writeObject(o);
        } catch (IOException e) {
            System.out.println("Could not write object to process");
            e.printStackTrace();
            System.out.println(e);
        }

        return this;
    }

    public Object readObjectFromProcess() {
        try {
            return objIn.readObject();
        } catch (IOException e) {
            System.out.println(e);
            System.out.println("Error reading object");
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }

        return null;
    }

    // Deprecated
    // throttle is used as a balance between the advantage of
    // buffered read while not overloading memory
    public ArrayList<String> readTextFromProcess(int throttle) {
        // TODO: should use thread?
        // TODO: test throttle

        ArrayList<String> list = new ArrayList<String>();

        try {
            // TODO: make "END" env variable
            String s;
            while (throttle-- > 0 && (s = in.readLine()) != null && !s.equals(Globals.EOI_MSG))
                list.add(String.format("%s%n", s));
        } catch (IOException e) {
            System.out.println(e);
            System.out.println("IOException reading from node");
        }

        return list;
    }

    public void createStringOutputStream() {
        try {
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void createObjectOutputStream() {
        try {
            this.objOut = new ObjectOutputStream(this.clientSocket.getOutputStream());
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void createObjectInputStream() {
        try {
            this.objIn = new ObjectInputStream(this.clientSocket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void end() {
        try {
            if (out != null)    out.close();
            if (objIn != null)  objIn.close();
            if (objOut != null) objOut.close();
        } catch(IOException e) {
            System.out.println(e);
        }
    }
}

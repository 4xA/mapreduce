package com.atypon.Base.io;

import com.atypon.Globals;
import com.atypon.Base.node.Node;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;

/**
 * NodeSocketHandler is a class that handles
 * all communications with a network socket
 * from starting a connection to send and receiving
 * data over the network.
 * <p>
 *     This simplifying class allows for easier
 *     socket communication and separation of
 *     error handling.
 * </p>
 * @author Asa Abbad
 */
public class NodeSocketHandler {
    private Node node;
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private ObjectInputStream objIn;
    private ObjectOutputStream objOut;

    /**
     * Instantiate a {@link NodeSocketHandler} class
     * for a {@link Node}.
     * @param node  the {@link Node} this handler will take care of
     */
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

    /**
     * Send text array over socket.
     * @param text  {@link String}[] to be sent
     * @return {@link NodeSocketHandler} for chaining
     */
    public NodeSocketHandler writeTextToProcess(String[] text) {
        // TODO: throttle input so not to overload heap
        for (String s : text)
            out.println(s);
        out.flush();

        return this;
    }

    /**
     * Send object over socket.
     * @param o {@link Object} to be sent
     * @return {@link NodeSocketHandler} for chaining
     */
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

    /**
     * Read object over socket.
     * @return  {@link Object} to be read
     */
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

    /**
     * DISCONTINUED
     * @param throttle
     * @return
     */
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

    /**
     * Create an output stream for {@link String} output.
     * @throws {@link IllegalAccessException}   <b></b>if {@link NodeSocketHandler#createObjectOutputStream()}
     * has already been called</b>
     */
    public void createStringOutputStream() throws IllegalAccessException {
        try {
            if (objOut != null)
                throw new IllegalAccessException("trying to create a String output stream when an Object stream is already present");
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    /**
     * Create an output stream for {@link Object} output.
     * @throws {@link IllegalAccessException}   <b></b>if {@link NodeSocketHandler#createStringOutputStream()
     * has already been called</b>
     */
    public void createObjectOutputStream() throws IllegalAccessException {
        try {
            if (out != null)
                throw new IllegalAccessException("trying to create a String output stream when an Object stream is already present");
            this.objOut = new ObjectOutputStream(this.clientSocket.getOutputStream());
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    /**
     * Create an input stream for {@link Object} output.
     */
    public void createObjectInputStream() {
        try {
            this.objIn = new ObjectInputStream(this.clientSocket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Safely close all resources held by this socket handler
     */
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

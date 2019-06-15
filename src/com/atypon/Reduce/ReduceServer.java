package com.atypon.Reduce;

import com.atypon.Globals;
import com.atypon.Map.Pair;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class ReduceServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private ObjectInputStream objIn;
    private ObjectOutputStream objOut;

    public void start(int port) throws IOException, ClassNotFoundException {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new IOException(port + "");
        }
        System.out.println("waiting for connection...");
        clientSocket = serverSocket.accept();

        System.out.println("connected...");

        objOut = new ObjectOutputStream(clientSocket.getOutputStream());
        objIn = new ObjectInputStream(clientSocket.getInputStream());

        doIO();
    }

    public void stop() throws IOException {
        objIn.close();
        objOut.close();
        clientSocket.close();
        serverSocket.close();
    }

    private void doIO() throws IOException, ClassNotFoundException {
        Object input;
        Pair p;

        LinkedList<Pair> list = new LinkedList<Pair>();
        while (true) {
            try {
                input = objIn.readObject();

                if (input.equals(Globals.END_MSG))
                    break;
                else if (input.equals(Globals.SND_MSG)) {
                    System.out.println("writing...");

                    try {
                        p = list.removeFirst();
                    } catch (NoSuchElementException e) {
                        p = null;
                    }

                    if (p != null)
                        objOut.writeObject(p);
                    else
                        objOut.writeObject(Globals.EOF_MSG);
                }
                else {
                    System.out.println("reading...");
                    p = (Pair) input;
                    list.add(p);
                }
            } catch (IOException e) {
                continue;
            }
        }

        stop();
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        try {
            new ReduceServer().start(port);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

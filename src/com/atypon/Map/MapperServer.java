package com.atypon.Map;

import com.atypon.Globals;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class MapperServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedReader in;
    private ObjectOutputStream objOut;

    private final ArrayList<Pair> list = new ArrayList<Pair>();

    public void start(int port) throws IOException {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new IOException(port + "");
        }
        clientSocket = serverSocket.accept();

        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        objOut = new ObjectOutputStream(clientSocket.getOutputStream());

        read();
    }

    public void stop() throws IOException {
        in.close();
        objOut.close();
        clientSocket.close();
        serverSocket.close();
    }

    private void read() throws IOException {
        Object value;

        String inputLine;
        while ((inputLine = in.readLine()) != null) {

            // STOP SIGNAL
            if (inputLine.equals(Globals.EOI_MSG)) {
                objOut.writeObject(list);

                stop();
            }

            value = Mapper.map(inputLine);
            list.add(new Pair(inputLine, new Object[] { value }));
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

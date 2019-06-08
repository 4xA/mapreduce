package com.atypon.Map;

import com.atypon.Globals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class MapperServer {
    private static int count = 0;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    public void start(int port) throws IOException {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new IOException(port + "");
        }
        clientSocket = serverSocket.accept();

        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        // Send connection acknowledgment
        out.println(Globals.ACK_MSG);

        // Debug
        out.println(String.format("DEBUG: Server running at port (%d)", port));

        read();
    }

    public void stop() throws IOException {
        in.close();
        out.close();
        clientSocket.close();
        serverSocket.close();
    }

    private void read() throws IOException {
        StringBuilder sb = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if (inputLine.equals(Globals.END_MSG)) {
                out.println(String.format("DEBUG: %s", sb.toString()));
                stop();
            }

            sb.append(inputLine);
        }
    }

    public static void main(String[] args) {
        int port;
        if (args.length == 1)
            port = Integer.parseInt(args[0]);
        else
            port = 6000 + count++;

        try {
            new MapperServer().start(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

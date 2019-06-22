package com.atypon.Reduce;

import java.io.*;
import java.net.ServerSocket;

/**
 * {@link ReduceServer} is the low-level server
 * running as a ReduceNode.
 * @author Asa Abbad
 */
public class ReduceServer {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            int clientCount = 1;

            Thread[] threads = new Thread[clientCount];
            int w = 0;
            while (clientCount > 0) {
                threads[w] = new ReduceThread(serverSocket.accept());
                threads[w].start();
                w++;
                clientCount--;
            }

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

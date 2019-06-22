package com.atypon.Map;

import com.atypon.Globals;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class MapperServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedReader in;
    private ObjectOutputStream objOut;
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

        // Read
        String[] keys = read();

        // Map
        mappedData = map(keys);

        // Print Output to main process
        writeOutput();

        // Stop Server
        stop();
    }

    public void stop() throws IOException {
        in.close();
        objOut.close();
        clientSocket.close();
        serverSocket.close();
    }

    private String[] read() throws IOException {
        ArrayList<String> list = new ArrayList<String>();

        String inputLine;
        Object o;
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

            list.add(inputLine);
        }

        return list.toArray(new String[0]);
    }

    private Pair[] map(String[] keys) {
        Pair[] ret = new Pair[keys.length];

        int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        MapTask[] tasks = new MapTask[threadCount];

        double count = keys.length;
        int[] countPerThread = new int[threadCount];

        for (int i = 0; i < threadCount; i++)
            countPerThread[i] = (int) ( Math.floor(count / threadCount) + (i+1 <= count%threadCount ? 1 : 0) );

        int startIndex = 0;
        for (int i = 0; i < threadCount; i++) {
            int endIndex = startIndex + countPerThread[i];

            tasks[i] = new MapTask(Arrays.copyOfRange(keys, startIndex, endIndex));
            threads[i] = new Thread(tasks[i]);
            threads[i].start();

            startIndex = endIndex;
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int w = 0;
        for (MapTask task : tasks)
            for (Pair p : task.getMapped())
                ret[w++] = p;

        return ret;
    }

    private void writeOutput() throws IOException {
        objOut.writeObject(mappedData);

        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        objOut.writeObject(heapMemoryUsage.getUsed());
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

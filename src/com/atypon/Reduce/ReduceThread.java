package com.atypon.Reduce;

import com.atypon.Globals;
import com.atypon.Map.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;

public class ReduceThread extends Thread {
    private Socket clientSocket;
    private ObjectInputStream objIn;
    private ObjectOutputStream objOut;

    public ReduceThread(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            System.out.println("connected...");

            objOut = new ObjectOutputStream(clientSocket.getOutputStream());
            objIn = new ObjectInputStream(clientSocket.getInputStream());

            doIO();

            closeServer();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void closeServer() throws IOException {
        objIn.close();
        objOut.close();
        clientSocket.close();
    }

    private void doIO() throws IOException, ClassNotFoundException {
        Object input;
        Pair p;

        LinkedList<Pair> list = new LinkedList<>();

        while (true) {
            try {
                input = objIn.readObject();

                if (input.equals(Globals.END_MSG))
                    break;
                else if (input.equals(Globals.SND_MSG)) {
                    System.out.println("writing...");

                    Pair[] sendPairs = new Pair[list.size()];

                    sendPairs = reduce(list.toArray(new Pair[0]));

                    objOut.writeObject(sendPairs);
                    objOut.writeObject(Globals.EOF_MSG);

                    MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
                    objOut.writeObject(heapMemoryUsage.getUsed());
                }
                else {
                    System.out.println("reading...");
                    Pair[] pairs = (Pair[]) input;

                    list.addAll(Arrays.asList(pairs));
                }
            } catch (IOException e) {
                continue;
            }
        }
    }

    private Pair[] reduce(Pair[] pairs) {
        Pair[] ret = new Pair[pairs.length];

        int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        ReduceTask[] tasks = new ReduceTask[threadCount];

        double count = pairs.length;
        int[] countPerThread = new int[threadCount];

        for (int i = 0; i < threadCount; i++)
            countPerThread[i] = (int) ( Math.floor(count / threadCount) + (i+1 <= count%threadCount ? 1 : 0) );

        int startIndex = 0;
        for (int i = 0; i < threadCount; i++) {
            int endIndex = startIndex + countPerThread[i];

            tasks[i] = new ReduceTask(Arrays.copyOfRange(pairs, startIndex, endIndex));
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
        for (ReduceTask task : tasks)
            for (Pair p : task.getReduced())
                ret[w++] = p;

        return ret;
    }
}

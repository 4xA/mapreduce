package com.atypon.Reduce;

import com.atypon.Globals;
import com.atypon.Map.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

                    for (int i = 0; i < list.size(); i++) {
                        p = list.get(i);
                        sendPairs[i] = Reducer.reduce(p.getKey(), (Object[]) p.getValue());
                    }

                    objOut.writeObject(sendPairs);
                    objOut.writeObject(Globals.EOF_MSG);
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
}

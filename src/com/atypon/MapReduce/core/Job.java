package com.atypon.MapReduce.core;

import com.atypon.MapReduce.node.MapNode;
import com.atypon.MapReduce.util.InputReader;
import com.atypon.MapReduce.util.Splitter;

import java.io.IOException;
import java.util.ArrayList;

public class Job {
    private JobConfig config;
    private ArrayList<String> input;
    private MapNode[] mapNodes;

    public Job(JobConfig config) {
        this.config = config;
    }

    public void start() {
        // Read input
        readInput();

        for (String s : input)
            System.out.println(s);

        // Split input
        // TODO: Split input should become a stream
        splitInput();

        for (String s : input)
            System.out.println(s);

        // Create Map Nodes
        mapNodes = createMapNodes();

         // Write split input to map nodes
        writeToMapNodes();

        // Read from map nodes (connection confirmation)
        String s = readFromMapNodes();
        System.out.println(s);

        // End map node
        destroyMapNodes();
    }

    private void readInput() {
        input = InputReader.readLinesFromFile(config.getFileName());
    }

    private void splitInput() {
        ArrayList<String> words = new ArrayList<String>();

        for (String s : input)
            words.addAll(Splitter.split(s, config.getSplitters()));

        input = words;
    }

    private MapNode[] createMapNodes() {
        MapNode[] nodes = new MapNode[config.getMapNodesCount()];

        int port = 6000;
        for (int i = 0; i < config.getMapNodesCount(); i++) {
            nodes[i] = new MapNode(port++);

            // try other ports if used by other program (Handle IOException)
            for (int j = port; j < 65535; j++) {
                try {
                    nodes[i].createMapNode();
                    break;
                } catch (IOException e) {
                    System.out.println("Port busy\nTrying new port...");
                    port++;
                    continue;
                }
            }
        }

        return nodes;
    }

    private void writeToMapNodes() {
        double count = input.size();
        int nodeCount = config.getMapNodesCount();
        int countPerNode[] = new int[nodeCount];

        for (int i = 0; i < nodeCount; i++)
            countPerNode[i] = (int) ( Math.floor(count / nodeCount) + (i+1 <= count%nodeCount ? 1 : 0) );

        // distribute
        int startIndex = 0;
        for (int i = 0; i < nodeCount; i++) {
            int endIndex = startIndex + countPerNode[i];

            mapNodes[i].send(input.subList(startIndex, endIndex));

            startIndex = endIndex;
        }
    }

    private String readFromMapNodes() {
        StringBuilder sb = new StringBuilder();

        for (MapNode m : mapNodes)
            for (String s : m.receive(Integer.MAX_VALUE))
                sb.append(s);

        return sb.toString();
    }

    private void destroyMapNodes() {
        for (MapNode m : mapNodes)
            m.destroy();
    }
}

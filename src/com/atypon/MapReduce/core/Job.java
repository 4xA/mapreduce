package com.atypon.MapReduce.core;

import com.atypon.MapReduce.io.NodeIOHandler;
import com.atypon.MapReduce.io.NodeSocketHandler;
import com.atypon.MapReduce.node.MapNode;
import com.atypon.MapReduce.node.Node;
import com.atypon.MapReduce.util.InputReader;
import com.atypon.MapReduce.util.Splitter;

import java.util.ArrayList;

public class Job {
    private JobConfig config;
    private ArrayList<String> input;
    private MapNode[] mapNodes;
    private NodeIOHandler[] mapIOHandlers;
    private NodeSocketHandler[] mapSocketHandlers;

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

        // Wrap Map Nodes in IO handlers
        mapSocketHandlers = wrapNodeSocketHandlers(mapNodes);

         // Write split input to map nodes
        writeToMapNodes();

        // Read from map nodes (connection confirmation)
        String s = readFromMapNodes();
        System.out.println(s);

        // Close all map assets
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

        int port = 9000;
        for (int i = 0; i < config.getMapNodesCount(); i++) {
            nodes[i] = new MapNode(port++);
            nodes[i].createMapNode();
        }

        return nodes;
    }

    private NodeIOHandler[] wrapIOHandlers(Node[] nodes) {
        NodeIOHandler[] handlers = new NodeIOHandler[nodes.length];

        for (int i = 0; i < nodes.length; i++)
            handlers[i] = new NodeIOHandler(nodes[i]);

        return handlers;
    }

    private NodeSocketHandler[] wrapNodeSocketHandlers(Node[] nodes) {
         NodeSocketHandler[] handlers = new NodeSocketHandler[nodes.length];

        for (int i = 0; i < nodes.length; i++)
            handlers[i] = new NodeSocketHandler(nodes[i]);

        return handlers;
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

            mapSocketHandlers[i].writeToProcess(
                    input.subList(startIndex, endIndex)
            );

            mapSocketHandlers[i].writeToProcess(
                    new ArrayList<String>() {{ add("END"); }}
            );

            startIndex = endIndex;
        }
    }

    private String readFromMapNodes() {
        StringBuilder sb = new StringBuilder();

        for (NodeSocketHandler h : mapSocketHandlers) {
            for (String s : h.readFromProcess(Integer.MAX_VALUE))
                sb.append(s);
        }

        return sb.toString();
    }

    private void destroyMapNodes() {
        for (NodeSocketHandler h : mapSocketHandlers) {
            h.end();
        }
    }
}

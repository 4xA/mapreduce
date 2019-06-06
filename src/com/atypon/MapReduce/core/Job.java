package com.atypon.MapReduce.core;

import com.atypon.MapReduce.io.NodeIOHandler;
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
         mapIOHandlers = wrapIOHandlers(mapNodes);

         // Write splitted input to map nodes
        writeToMapNodes();

         for (NodeIOHandler h : mapIOHandlers) {
//             h.writeToProcess(
//                     new ArrayList<String>() {{
//                         add("HELLO PLZ W0RK\n");
//                         add("I ActuALLy good\n");
//                         add("PLZ Do Ting\n");
//                         add("And aLso do me nic graphics\n");
//                         add("PLZ\n");
//                     }}
//             );

             ArrayList<String> list;

             while ((list = h.readFromProcess(2)).size() > 0)
                for (String s : list)
                    System.out.println(s);
         }

    }

    private void readInput() {
        input = InputReader.readLinesFromFile(config.getFileName());
    }

    private void splitInput() {
        ArrayList<String> words = new ArrayList<String>();

        for (String s : input)
            words.addAll(Splitter.split(s, ":", "-", ".", "***"));

        input = words;
    }

    private MapNode[] createMapNodes() {
        MapNode[] nodes = new MapNode[config.getMapNodesCount()];

        for (int i = 0; i < config.getMapNodesCount(); i++) {
            nodes[i] = new MapNode();
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

            mapIOHandlers[i].writeToProcess(
                    input.subList(startIndex, endIndex)
            );

            startIndex = endIndex;
        }
    }
}

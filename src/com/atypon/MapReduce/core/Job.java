package com.atypon.MapReduce.core;

import com.atypon.Globals;
import com.atypon.Map.Pair;
import com.atypon.MapReduce.node.MapNode;
import com.atypon.MapReduce.node.Node;
import com.atypon.MapReduce.node.ReduceNode;
import com.atypon.MapReduce.util.InputReader;
import com.atypon.MapReduce.util.Splitter;

import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class Job implements java.io.Serializable {
    private JobConfig config;
    private ArrayList<String> input;
    private MapNode[] mapNodes;
    private ReduceNode[] reduceNodes;

    public Job(JobConfig config) {
        this.config = config;
        mapNodes = new MapNode[this.config.getMapNodesCount()];
        reduceNodes = new ReduceNode[this.config.getReduceNodesCount()];
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

        createMapNodes();
        input = null; // clear memory
        createReduceNodes();

        stopNodes(mapNodes);
        stopNodes(reduceNodes);
    }

    private void readInput() {
        input = InputReader.readLinesFromFile(config.getFileName());
    }

    private void splitInput() {
        ArrayList<String> words = new ArrayList<String>();

        for (String s : input)
            words.addAll(Splitter.split(s, config.getSplitterRegex()));

        input = words;
    }

    private void createMapNodes() {
        // TODO: only create needed nodes
        double count = input.size();
        int nodeCount = config.getMapNodesCount();
        int countPerNode[] = new int[nodeCount];

        for (int i = 0; i < nodeCount; i++)
            countPerNode[i] = (int) ( Math.floor(count / nodeCount) + (i+1 <= count%nodeCount ? 1 : 0) );

        int startIndex = 0;
        for (int i = 0; i < nodeCount; i++) {
            int endIndex = startIndex + countPerNode[i];

            mapNodes[i] = new MapNode(
                    config.getMapServerPort() + i,
                    input.subList(startIndex, endIndex).toArray(new String[0])
            );

            mapNodes[i].run();

            startIndex = endIndex;
        }
    }

    private void createReduceNodes()  {
        // TODO: only create needed nodes

        int port = config.getReduceServerPort();

        for (int i = 0; i < config.getReduceNodesCount(); i++) {
            reduceNodes[i] = new ReduceNode(port++);
            reduceNodes[i].run();
        }
    }

    private void stopNodes(Node[] nodes) {
        for (Node n : nodes)
            n.destroy();
    }
}

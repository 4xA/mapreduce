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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Job implements java.io.Serializable {
    private JobConfig config;
    private ArrayList<String> input;
    private MapNode[] mapNodes;
    private ReduceNode[] reduceNodes;
    private int[] reducePorts;
    private Pair[] mappedData;

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

        // Create Map Nodes
        createMapNodes();
        input = null; // clear memory

        // Combine Data
        mappedData = combineData();
        System.out.println(Arrays.toString(mappedData));

        // Partition


        // Create ReduceNodes Nodes
        createReduceNodes();

        assignReducePorts();


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

    private void assignReducePorts() {
        reducePorts = new int[reduceNodes.length];

        int port = config.getReduceServerPort();
        for (int i = 0; i < reducePorts.length; i++) {
            reducePorts[i] = port++;
        }
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

    private Pair[] combineData() {
        ArrayList<Pair> list = new ArrayList<Pair>();

        HashMap<String, ArrayList<Object>> combineMap = new HashMap<String, ArrayList<Object>>();

        for (int i = 0; i < mapNodes.length; i++) {
            MapNode node = mapNodes[i];

            for (Pair p : node.getMappedData())
                if (!combineMap.containsKey(p.getKey())) {
                    ArrayList<Object> arrayList = new ArrayList<Object>();
                    arrayList.add(p.getValue());

                    combineMap.put(p.getKey(), arrayList);
                } else {
                    combineMap.get(p.getKey()).add(p.getValue());
                }
        }

        // conver HashMap to an array of pairs
        for (String key : combineMap.keySet()) {
            list.add(
                    new Pair(key, combineMap.get(key).toArray(new Object[0]))
            );
        }

        return list.toArray(new Pair[0]);
    }

    private void createReduceNodes()  {
        // TODO: only create needed nodes

        double count = mappedData.length;
        int nodeCount = config.getReduceNodesCount();
        int countPerNode[] = new int[nodeCount];

        for (int i = 0; i < nodeCount; i++)
            countPerNode[i] = (int) ( Math.floor(count / nodeCount) + (i+1 <= count%nodeCount ? 1 : 0) );

        int port = config.getReduceServerPort();

        int startIndex = 0;
        for (int i = 0; i < config.getReduceNodesCount(); i++) {
            int endIndex = startIndex + countPerNode[i];

            Pair[] send = Arrays.copyOfRange(mappedData, startIndex, endIndex);

            reduceNodes[i] = new ReduceNode(
                    port++,
                    send
                    );
            reduceNodes[i].run();

            startIndex = endIndex;
        }
    }

    private void stopNodes(Node[] nodes) {
        for (Node n : nodes)
            n.destroy();
    }
}

package com.atypon.MapReduce.core;

import com.atypon.Globals;
import com.atypon.Map.Pair;
import com.atypon.MapReduce.node.MapNode;
import com.atypon.MapReduce.node.Node;
import com.atypon.MapReduce.node.ReduceNode;
import com.atypon.MapReduce.util.FileOutputWriter;
import com.atypon.MapReduce.util.InputReader;
import com.atypon.MapReduce.util.Splitter;

import java.util.*;

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

        for (ReduceNode node : reduceNodes)
            for (Pair p : node.getReducedData())
                System.out.println(p);
//            System.out.println(Arrays.toString(node.getReducedData()));

        // Write output to file
        FileOutputWriter writer = new FileOutputWriter(Globals.OUTPUT_FILE_NAME);
        for (ReduceNode node : reduceNodes)
            writer.write(node.getReducedData());
        writer.close();

        String performanceString = generatePerformanceAnalysisString();
        System.out.println(performanceString);

        writer = new FileOutputWriter(Globals.PERFORMANCE_FILE_NAME);
        writer.write(performanceString);
        writer.close();
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
        LinkedList<Pair> list = new LinkedList<Pair>();

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

        Pair[] ret = list.toArray(new Pair[0]);
        Arrays.sort(ret);

        return ret;
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

    private String generatePerformanceAnalysisString() {
        StringBuilder s = new StringBuilder("");

        s.append("<<EXECUTION TIMES>>\n\n");

        // Execution time per node
        s.append("Execution time per Map Nodes (Process creation and destruction time overhead inclusive)\n");
        for (int i = 0; i < mapNodes.length; i++)
            s.append(
                    String.format("\tMapNode[%d]: %s%n", i, mapNodes[i].getExcecutionTimeFormated())
            );

        s.append("\n");
        s.append("Execution time per Reduce Nodes (Process creation and destruction time overhead inclusive)...\n");
        for (int i = 0; i < reduceNodes.length; i++)
            s.append(
                    String.format("\tReduceNode[%d]: %s%n", i, reduceNodes[i].getExcecutionTimeFormated())
            );

        s.append("\n");

        s.append("<<WORK DISTRIBUTION>>\n\n");

        // Keys sent to map nodes
        s.append("Number of keys sent to map nodes...\n");
        for (int i = 0; i < mapNodes.length; i++)
            s.append (
                    String.format("\tMapNode[%d]: %d%n", i, mapNodes[i].getData().length)
            );

        s.append("\n");
        // Combined mapped data sent to reduces nodes
        s.append("Mapped keys sent to reduce nodes...\n");
        for (int i = 0; i < reduceNodes.length; i++)
            s.append(
                    String.format("\tReduceNode[%d]: %d%n", i, reduceNodes[i].getMappedData().length)
            );

        s.append("\n");

        s.append("<<MEMORY FOOTPRINT>>\n\n");

        // Memory used per Node
        s.append("Heap memory used per Map Node...\n");
        for (int i = 0; i < mapNodes.length; i++)
            s.append(
                    String.format("\tMapNode[%d]: %d bytes%n", i, mapNodes[i].getHeapMemoryUsed())
            );

        s.append("\n");
        s.append( "Heap memory used per Map Node...\n");
        for (int i = 0; i < reduceNodes.length; i++)
            s.append(
                    String.format("\tReduceNode[%d]: %d bytes%n", i, reduceNodes[i].getHeapMemoryUsed())
            );

        return s.toString();
    }
}

package com.atypon.Base.core;

import com.atypon.Base.util.Pair;
import com.atypon.Base.node.MapNode;
import com.atypon.Base.node.Node;
import com.atypon.Base.node.ReduceNode;
import com.atypon.Base.util.FileOutputWriter;
import com.atypon.Base.util.InputReader;
import com.atypon.Base.util.Splitter;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Job is a representation of a complete
 * task given to the MapReduce system. It
 * is also the entry point of MapReduce.
 * @author  Asa Abbad
 */
public class Job {
    private JobConfig config;
    private ArrayList<String> input;
    private MapNode[] mapNodes;
    private ReduceNode[] reduceNodes;
    private Pair[] mappedData;
    private long startTime;
    private long endTime;

    /**
     * Instantiate {@link Job} object with specified arguments
     * @param config     {@link JobConfig} Job configuration class
     */
    public Job(JobConfig config) {
        this.config = config;
        mapNodes = new MapNode[this.config.getMapNodesCount()];
        reduceNodes = new ReduceNode[this.config.getReduceNodesCount()];
    }

    /**
     * Entry point of MapReduce system
     */
    public void start() {
        this.startTime = System.currentTimeMillis();

        // Read input
        readInput();

        // Split input
        // TODO: Split input should become a stream
        splitInput();

        // Create Map Nodes
        System.out.println("Creating and executing map nodes...");
        createMapNodes();

        // Run Map Nodes
        runNodes(mapNodes);
        input = null; // clear memory

        // Combine Data
        System.out.println("\nCombining and partitioning data...");

        mappedData = combineData();

        for (int i = 0; i < mappedData.length && i < 20; i++) {
            System.out.println(mappedData[i]);
        }
        if (mappedData.length > 20)
            System.out.println("...\t[[TRIMMED]]\n"+ mappedData[mappedData.length-1]);

        // Create ReduceNodes Nodes
        System.out.println("\nCreating and executing reduce nodes...");
        createReduceNodes();
        runNodes(reduceNodes);

        int reduceCount = 0;
        outer: for (int i = 0; i < reduceNodes.length; i++) {
            ReduceNode node = reduceNodes[i];

            for (Pair p : node.getReducedData()) {
                System.out.println(p);
                reduceCount++;
                if (reduceCount > 20) {
                    System.out.println("...\t[[TRIMMED]]");
                    break outer;
                }
            }
        }

        // Write output to file
        System.out.println("\nWriting to output file...");
        FileOutputWriter writer = new FileOutputWriter(config.getOutputFileName());
        for (ReduceNode node : reduceNodes)
            writer.write(node.getReducedData());
        writer.close();

        this.endTime = System.currentTimeMillis();

        System.out.println("\nGenerating and writing performance analysis to file...");
        String performanceString = generatePerformanceAnalysisString();
        System.out.println(performanceString);

        writer = new FileOutputWriter(config.getPerformanceFileName());
        writer.write(performanceString);
        writer.close();
    }

    private void readInput() {
        input = InputReader.readLinesFromFile(config.getInputFileName());
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

            startIndex = endIndex;
        }
    }

    private void runNodes(Node[] nodes) {
        Thread[] threads = new Thread[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            Node node = nodes[i];

            threads[i] = new Thread () {
              public void run() {
                  node.run();
              }
            };

            threads[i].start();
        }

        for (Thread t : threads)
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
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

        // Convert HashMap to an array of pairs
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

            startIndex = endIndex;
        }
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
        s.append(
                String.format("Execution time of entire system: %s%n", this.getFullTime())
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

    private String getFullTime() {
        long time = this.endTime - this.startTime;

        long seconds = TimeUnit.MILLISECONDS.toSeconds(time);
        long milliseconds = time - TimeUnit.SECONDS.toMillis(seconds);

        return String.format("%d.%d", seconds, milliseconds);
    }
}

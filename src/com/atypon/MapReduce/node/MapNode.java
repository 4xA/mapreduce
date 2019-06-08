package com.atypon.MapReduce.node;

import com.atypon.Map.MapperServer;

import java.io.IOException;

public class MapNode extends Node {
    public MapNode(int port) {
        super(port);
    }

    public void createMapNode() throws IOException {
        createProcess(MapperServer.class);
    }
}

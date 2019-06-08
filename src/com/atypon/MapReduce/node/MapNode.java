package com.atypon.MapReduce.node;

import com.atypon.Map.MapperServer;

public class MapNode extends Node {
    public MapNode(int port) {
        super(port);
    }

    public void createMapNode() {
        createProcess(MapperServer.class);
    }
}

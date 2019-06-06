package com.atypon.MapReduce.node;

import com.atypon.Map.Mapper;

public class MapNode extends Node {
    public void createMapNode() {
        createProcess(Mapper.class);
    }
}

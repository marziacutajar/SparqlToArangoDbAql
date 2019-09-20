package com.aql.algebra.resources;

import com.aql.algebra.NodeVisitor;

import java.util.List;

public class GraphIterationResource {/*implements Resource{
    @Override
    public void visit(NodeVisitor resVisitor) { resVisitor.visit(this); }*/

    public enum TraversalDirection{
        OUTBOUND, INBOUND, ANY
    }

    String vertexVar;
    String edgeVar;
    String pathVar;
    Integer min;
    Integer max;
    String startVertex;
    TraversalDirection direction;
    List<String> edgeCollections;
    String graph;

    public GraphIterationResource(String vertexVar, String edgeVar, String pathVar, Integer min, Integer max, String startVertex, TraversalDirection direction, List<String> edgeCollections){

    }

    public GraphIterationResource(String vertexVar, String edgeVar, String pathVar, Integer min, Integer max, String startVertex, TraversalDirection direction, String graph){

    }
}

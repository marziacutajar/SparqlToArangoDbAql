package com.aql.algebra.resources;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.NodeVisitor;

import java.util.List;

public class GraphIterationResource implements Resource{

    public enum TraversalDirection{
        OUTBOUND, INBOUND, ANY
    }

    private String vertexVar;
    private String edgeVar;
    private String pathVar;
    private Integer min;
    private Integer max;
    private String startVertex;
    private TraversalDirection direction;
    private List<String> edgeCollections;
    private String graph;

    public GraphIterationResource(String vertexVar, String edgeVar, String pathVar, Integer min, Integer max, String startVertex, TraversalDirection direction, List<String> edgeCollections){
        this.vertexVar = vertexVar;
        this.edgeVar = edgeVar;
        this.pathVar = pathVar;
        this.min = min;
        this.max = max;
        this.startVertex = startVertex;
        this.direction = direction;
        this.edgeCollections = edgeCollections;
    }

    public GraphIterationResource(String vertexVar, String edgeVar, String pathVar, Integer min, Integer max, String startVertex, TraversalDirection direction, String graph){
        this.vertexVar = vertexVar;
        this.edgeVar = edgeVar;
        this.pathVar = pathVar;
        this.min = min;
        this.max = max;
        this.startVertex = startVertex;
        this.direction = direction;
        this.graph = graph;
    }

    @Override
    public void visit(NodeVisitor resVisitor) { resVisitor.visit(this); }

    @Override
    public String getName() { return AqlConstants.keywordFor; }

    public String getVertexVar(){
        return vertexVar;
    }

    public String getEdgeVar(){
        return edgeVar;
    }

    public String getPathVar(){
        return pathVar;
    }

    public Integer getMin(){
        return min;
    }

    public Integer getMax(){
        return max;
    }

    public String getStartVertex(){
        return startVertex;
    }

    public List<String> getEdgeCollections(){
        return edgeCollections;
    }

    public String getGraph(){
        return graph;
    }

    public TraversalDirection getDirection(){
        return direction;
    }

    public String getDirectionAsString(){
        switch (direction){
            case OUTBOUND:
                return "OUTBOUND";
            case INBOUND:
                return "INBOUND";
            case ANY:
                return "ANY";
            default:
                throw new UnsupportedOperationException("Unsupported traversal direction");
        }
    }
}

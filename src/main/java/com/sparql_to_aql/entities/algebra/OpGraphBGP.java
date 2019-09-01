package com.sparql_to_aql.entities.algebra;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

public class OpGraphBGP extends OpBGP {

    private final Node graphNode;

    public OpGraphBGP(Node graphNode, BasicPattern pattern)
    {
        super(pattern);
        this.graphNode = graphNode;
    }

    public Node getGraphNode() { return graphNode; }

    @Override
    public String getName() { return "graph_bgp"; }
}

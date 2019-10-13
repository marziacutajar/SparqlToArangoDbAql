package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.ExprList;
import com.aql.algebra.operators.*;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.entities.algebra.OpGraphBGP;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.*;
import java.util.*;

public class ArqToAqlAlgebraVisitor_GraphVersion extends ArqToAqlAlgebraVisitor {

    public ArqToAqlAlgebraVisitor_GraphVersion(List<String> defaultGraphNames, List<String> namedGraphs){
        super(defaultGraphNames, namedGraphs);
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new ArrayList<>();
    }

    @Override
    public void visit(OpBGP opBgp){
        boolean bgpWithGraphNode = false;
        Node graphNode = null;
        if(opBgp instanceof OpGraphBGP){
            bgpWithGraphNode = true;
            OpGraphBGP graphBGP = (OpGraphBGP) opBgp;
            graphNode = graphBGP.getGraphNode();
        }

        AqlQueryNode currAqlNode = null;
        Map<String, String> usedVars = new HashMap<>();
        boolean firstTripleBeingProcessed = true;

        //using this variable, we will make sure the graph name of every triple matching the BGP is in the same graph
        String outerGraphVarToMatch = "";

        for(Triple triple : opBgp.getPattern().getList()){
            ExprList subjectFilterConditions = new ExprList();
            Node subject = triple.getSubject();
            String startVertex;
            //if the subject of the triple isn't bound already - we need an extra for loop
            if(subject.isURI() || (subject.isVariable() && !usedVars.containsKey(subject.getName()))){
                String forloopVar = forLoopVarGenerator.getNew();
                AqlQueryNode new_forloop = new IterationResource(forloopVar, com.aql.algebra.expressions.Var.alloc(ArangoDatabaseSettings.GraphModel.rdfResourcesCollectionName));
                RewritingUtils.ProcessTripleNode(triple.getSubject(), forloopVar, subjectFilterConditions, usedVars, false);
                if(subjectFilterConditions.size() > 0){
                    new_forloop = new com.aql.algebra.operators.OpFilter(subjectFilterConditions, new_forloop);
                }

                if(currAqlNode == null)
                    currAqlNode = new_forloop;
                else
                    currAqlNode = new OpNest(currAqlNode, new_forloop);

                startVertex = forloopVar;
            }
            else {
                startVertex = usedVars.get(subject.getName());
            }

            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();
            String iterationVertexVar = graphForLoopVertexVarGenerator.getNew();
            String iterationEdgeVar = graphForLoopEdgeVarGenerator.getNew();
            String iterationPathVar = graphForLoopPathVarGenerator.getNew();

            //if this is the first for loop and there are named graphs specified, add filters for those named graphs
            //outerGraphVarToMatch = AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE);

            /*if(bgpWithGraphNode){
                if(graphNode.isVariable()){
                    AddGraphFilters(namedGraphNames, iterationVar, filterConditions);

                    //bind graph var
                    usedVars.put(graphNode.getName(), AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME));
                }
                else{
                    //add filter with specific named graph
                    filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(outerGraphVarToMatch), new Const_String(graphNode.getURI())));
                }
            }
            else{
                //if there are default graphs specified, filter by those
                if(defaultGraphNames.size() > 0){
                    AddGraphFilters(defaultGraphNames, iterationVar, filterConditions);
                }
            }*/
            //make sure that graph name for consecutive triples matches the one of the first triple
            //filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), com.aql.algebra.expressions.Var.alloc(outerGraphVarToMatch)));

            AqlQueryNode aqlNode = new GraphIterationResource(iterationVertexVar, iterationEdgeVar, iterationPathVar, 1, 1, startVertex, GraphIterationResource.TraversalDirection.OUTBOUND, List.of(ArangoDatabaseSettings.GraphModel.rdfEdgeCollectionName));

            RewritingUtils.ProcessTripleNode(triple.getPredicate(), AqlUtils.buildVar(iterationPathVar, "edges[0]", ArangoAttributes.PREDICATE), filterConditions, usedVars, true);
            RewritingUtils.ProcessTripleNode(triple.getObject(), AqlUtils.buildVar(iterationPathVar, "vertices[1]"), filterConditions, usedVars, false);

            if(filterConditions.size() > 0)
                aqlNode = new com.aql.algebra.operators.OpFilter(filterConditions, aqlNode);

            if(currAqlNode == null) {
                currAqlNode = aqlNode;
            }
            else {
                currAqlNode = new OpNest(currAqlNode, aqlNode);
            }

            firstTripleBeingProcessed = false;
        }

        //add used vars in bgp to list
        SetSparqlVariablesByOp(opBgp.hashCode(), usedVars);
        createdAqlNodes.add(currAqlNode);
    }
}

package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.ExprList;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.Expr_Equals;
import com.aql.algebra.operators.*;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.entities.BoundAqlVars;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import com.sparql_to_aql.utils.VariableGenerator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.*;
import java.util.*;

public class ArqToAqlAlgebraVisitor_GraphApproach extends ArqToAqlAlgebraVisitor {

    public ArqToAqlAlgebraVisitor_GraphApproach(List<String> defaultGraphNames, List<String> namedGraphs){
        super(defaultGraphNames, namedGraphs, ArangoDataModel.G);
    }

    public ArqToAqlAlgebraVisitor_GraphApproach(List<String> defaultGraphNames, List<String> namedGraphs, VariableGenerator forLoopVarGen, VariableGenerator assignmentVarGen, VariableGenerator graphVertexVarGen, VariableGenerator graphEdgeVarGen, VariableGenerator graphPathVarGen){
        super(defaultGraphNames, namedGraphs, ArangoDataModel.G, forLoopVarGen, assignmentVarGen, graphVertexVarGen, graphEdgeVarGen, graphPathVarGen);
    }

    //TODO try to improve runtime
    // try using prune..
    // try using INBOUND when we know the object but not the subject!!
    // consider using OPTIONS {bfs:true}  in graph traversal.. maybe we can improve performance somehow..
    @Override
    public void visit(OpBGP opBgp){
        AqlQueryNode currAqlNode = null;
        Map<String, BoundAqlVars> usedVars = new HashMap<>();

        for(Triple triple : opBgp.getPattern()){
            ExprList subjectFilterConditions = new ExprList();
            Node subject = triple.getSubject();
            String startVertex;
            //if the subject of the triple isn't bound already - we need an extra for loop
            if(subject.isURI() || (subject.isVariable() && !usedVars.containsKey(subject.getName()))){
                String forloopVar = forLoopVarGenerator.getNew();
                AqlQueryNode new_forloop = new IterationResource(forloopVar, new ExprVar(ArangoDatabaseSettings.GraphModel.rdfResourcesCollectionName));
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
                startVertex = usedVars.get(subject.getName()).getFirstVarName();
            }

            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();
            String iterationVertexVar = graphForLoopVertexVarGenerator.getNew();
            String iterationEdgeVar = graphForLoopEdgeVarGenerator.getNew();
            String iterationPathVar = graphForLoopPathVarGenerator.getNew();

            AqlQueryNode aqlNode = new GraphIterationResource(iterationVertexVar, iterationEdgeVar, iterationPathVar, 1, 1, startVertex, GraphIterationResource.TraversalDirection.OUTBOUND, List.of(ArangoDatabaseSettings.GraphModel.rdfEdgeCollectionName));

            //if there are default graphs specified, filter by those
            //we don't need to check that each triple matched by the BGP is in the same named graph.. since here we're using the default graph so all triples are considered to be in that one graph
            //TODO try to find a better performing approach instead of repeating these filter in each graph traversal
            // one option is to keep a list of graphs in the ArangoDB document of each resource, to know in which named graphs it is used
            // then if we have default or named graphs, we use a let assignment to hold all documents having one of the named graphs in their list, and another let assignment for the default graphs...
            // maybe do the same for the edges..
            if(defaultGraphNames.size() > 0){
                AddGraphFilters(defaultGraphNames, AqlUtils.buildVar(iterationPathVar, "edges[0]"), filterConditions);
            }

            RewritingUtils.ProcessTripleNode(triple.getPredicate(), AqlUtils.buildVar(iterationPathVar, "edges[0]", ArangoAttributes.PREDICATE), filterConditions, usedVars, false);
            RewritingUtils.ProcessTripleNode(triple.getObject(), AqlUtils.buildVar(iterationPathVar, "vertices[1]"), filterConditions, usedVars, true);

            if(filterConditions.size() > 0)
                aqlNode = new com.aql.algebra.operators.OpFilter(filterConditions, aqlNode);

            if(currAqlNode == null) {
                currAqlNode = aqlNode;
            }
            else {
                currAqlNode = new OpNest(currAqlNode, aqlNode);
            }
        }

        //add used vars in bgp to list
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opBgp, usedVars);
        createdAqlNodes.add(currAqlNode);
    }

    @Override
    public void visit(OpQuadPattern opQuadPattern){
        Node graphNode = opQuadPattern.getGraphNode();
        AqlQueryNode currAqlNode = null;
        Map<String, BoundAqlVars> usedVars = new HashMap<>();
        boolean firstTripleBeingProcessed = true;

        //using this variable, we will make sure the graph name of every triple matching the BGP is in the same graph
        String outerGraphVarToMatch = "";

        for(Triple triple : opQuadPattern.getBasicPattern()){
            ExprList subjectFilterConditions = new ExprList();
            Node subject = triple.getSubject();
            String startVertex;
            //if the subject of the triple isn't bound already - we need an extra for loop
            if(subject.isURI() || (subject.isVariable() && !usedVars.containsKey(subject.getName()))){
                String forloopVar = forLoopVarGenerator.getNew();
                AqlQueryNode new_forloop = new IterationResource(forloopVar, new ExprVar(ArangoDatabaseSettings.GraphModel.rdfResourcesCollectionName));
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
                startVertex = usedVars.get(subject.getName()).getFirstVarName();
            }

            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();
            String iterationVertexVar = graphForLoopVertexVarGenerator.getNew();
            String iterationEdgeVar = graphForLoopEdgeVarGenerator.getNew();
            String iterationPathVar = graphForLoopPathVarGenerator.getNew();

            AqlQueryNode aqlNode = new GraphIterationResource(iterationVertexVar, iterationEdgeVar, iterationPathVar, 1, 1, startVertex, GraphIterationResource.TraversalDirection.OUTBOUND, List.of(ArangoDatabaseSettings.GraphModel.rdfEdgeCollectionName));

            //TODO test below to make sure it works... same with BGP part - I'm not sure if the filters are correct cause of the edge doc attribute
            //if this is the first for loop and there are named graphs specified, add filters for those named graphs
            if(firstTripleBeingProcessed){
                outerGraphVarToMatch = AqlUtils.buildVar(iterationEdgeVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE);

                //TODO what if there are no named graphs defined in FROM NAMED??? in that case the inner graph pattern should return nothing no??
                if(graphNode.isVariable()){
                    AddGraphFilters(namedGraphNames, AqlUtils.buildVar(iterationPathVar, "edges[0]"), filterConditions);

                    //bind graph var
                    usedVars.put(graphNode.getName(), new BoundAqlVars(AqlUtils.buildVar(iterationEdgeVar, ArangoAttributes.GRAPH_NAME)));
                }
                else{
                    //add filter with specific named graph
                    filterConditions.add(new Expr_Equals(new ExprVar(outerGraphVarToMatch), new Const_String(graphNode.getURI())));
                }
            }
            else{
                //make sure that graph name for consecutive triples matches the one of the first triple
                filterConditions.add(new Expr_Equals(new ExprVar(AqlUtils.buildVar(iterationPathVar, "edges[0]", ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new ExprVar(outerGraphVarToMatch)));
            }

            RewritingUtils.ProcessTripleNode(triple.getPredicate(), AqlUtils.buildVar(iterationPathVar, "edges[0]", ArangoAttributes.PREDICATE), filterConditions, usedVars, false);
            RewritingUtils.ProcessTripleNode(triple.getObject(), AqlUtils.buildVar(iterationPathVar, "vertices[1]"), filterConditions, usedVars, true);

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
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opQuadPattern, usedVars);
        createdAqlNodes.add(currAqlNode);
    }
}

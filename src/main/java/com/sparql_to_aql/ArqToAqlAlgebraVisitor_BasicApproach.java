package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.ExprList;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.*;
import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpSequence;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.entities.algebra.OpGraphBGP;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.*;

import java.util.*;

public class ArqToAqlAlgebraVisitor_BasicApproach extends ArqToAqlAlgebraVisitor
{
    public ArqToAqlAlgebraVisitor_BasicApproach(List<String> defaultGraphNames, List<String> namedGraphs){
        super(defaultGraphNames, namedGraphs, ArangoDataModel.D);
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new OpSequence();
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
            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();

            String iterationVar = forLoopVarGenerator.getNew();
            AqlQueryNode aqlNode = new IterationResource(iterationVar, new ExprVar(ArangoDatabaseSettings.DocumentModel.rdfCollectionName));

            //if this is the first for loop and there are named graphs specified, add filters for those named graphs
            if(firstTripleBeingProcessed){
                outerGraphVarToMatch = AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE);

                if(bgpWithGraphNode){
                    if(graphNode.isVariable()){
                        AddGraphFilters(namedGraphNames, iterationVar, filterConditions);

                        //bind graph var
                        usedVars.put(graphNode.getName(), AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME));
                    }
                    else{
                        //add filter with specific named graph
                        filterConditions.add(new Expr_Equals(new ExprVar(outerGraphVarToMatch), new Const_String(graphNode.getURI())));
                    }
                }
                else{
                    //if there are default graphs specified, filter by those
                    if(defaultGraphNames.size() > 0){
                        AddGraphFilters(defaultGraphNames, iterationVar, filterConditions);
                    }
                }
            }
            else{
                //make sure that graph name for consecutive triples matches the one of the first triple
                filterConditions.add(new Expr_Equals(new ExprVar(AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new ExprVar(outerGraphVarToMatch)));
            }

            RewritingUtils.ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, iterationVar, filterConditions, usedVars);
            RewritingUtils.ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, iterationVar, filterConditions, usedVars);
            RewritingUtils.ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, iterationVar, filterConditions, usedVars);

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
        SetSparqlVariablesByOp(opBgp, usedVars);
        createdAqlNodes.add(currAqlNode);
    }
}

package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.*;
import com.aql.algebra.expressions.constants.Const_Object;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.Expr_Equals;
import com.aql.algebra.expressions.functions.Expr_In;
import com.aql.algebra.expressions.functions.Expr_Union;
import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpFilter;
import com.aql.algebra.operators.OpProject;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.entities.algebra.OpGraphBGP;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.vocabulary.RDF;

import java.util.*;

//TODO for this model, consider storing label and rdf:type in the uri itself
public class ArqToAqlAlgebraVisitor_GraphVersion extends ArqToAqlAlgebraVisitor {

    public ArqToAqlAlgebraVisitor_GraphVersion(List<String> defaultGraphNames, List<String> namedGraphs){
        super(defaultGraphNames, namedGraphs);
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new ArrayList<>();
    }

    @Override
    public void visit(OpBGP opBgp){
        /*boolean bgpWithGraphNode = false;
        Node graphNode = null;
        if(opBgp instanceof OpGraphBGP){
            bgpWithGraphNode = true;
            OpGraphBGP graphBGP = (OpGraphBGP) opBgp;
            graphNode = graphBGP.getGraphNode();
        }*/

        AqlQueryNode currAqlNode = null;
        Map<String, String> usedVars = new HashMap<>();
        //boolean firstTripleBeingProcessed = true;

        //using this variable, we will make sure the graph name of every triple matching the BGP is in the same graph
        //String outerGraphVarToMatch = "";

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

            Node predicate = triple.getPredicate();
            Node object = triple.getObject();

            boolean unionToRdfTypes = false;
            String rdfTypes_assigned_var = null;

            if(predicate.isURI() && predicate.getURI().equals(RDF.type.getURI())){
                //filter startVertex by rdf:type - FILTER 'type_uri_here' IN startVertex.rdf:type[*].value
                if(object.isURI()) {
                    currAqlNode = new OpFilter(new Expr_In(new Const_String(object.getURI()), Var.alloc(AqlUtils.buildVar(startVertex, AqlUtils.escapeString(ArangoAttributes.RDF_TYPE) + "[*]", ArangoAttributes.VALUE))), currAqlNode);
                }
                else if (object.isVariable()){
                    //if object is a variable, bind it by creating a loop over rdf:type array
                    String forloopVar = forLoopVarGenerator.getNew();
                    AqlQueryNode newForLoop = new IterationResource(forloopVar, Var.alloc(AqlUtils.buildVar(startVertex, AqlUtils.escapeString(ArangoAttributes.RDF_TYPE) + "[*]")));
                    usedVars.put(object.getName(), forloopVar);
                    currAqlNode = new OpNest(currAqlNode, newForLoop);
                }

                continue;
            }
            else if(predicate.isVariable()){
                String predicateVar = predicate.getName();
                if(usedVars.containsKey(predicateVar)){
                    //TODO consider not catering for this case atm
                    //ExprList filterConds = new ExprList();
                    //String boundVarName = usedVars.get(predicateVar);
                    //filterConds.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(currAqlVarName), com.aql.algebra.expressions.Var.alloc(boundVars.get(var_name))));
                }
                else if(object.isVariable() || object.isURI()){
                    unionToRdfTypes = true;
                    //when the predicate is an unbound variable, we have to get the applicable graph edges AS WELL AS the rdf:type values
                    //TODO I think we need to create two LET assignment with a union here..
                    String forloopVar = forLoopVarGenerator.getNew();
                    AqlQueryNode newForLoop = new IterationResource(forloopVar, Var.alloc(AqlUtils.buildVar(startVertex, AqlUtils.escapeString(ArangoAttributes.RDF_TYPE) + "[*]")));
                    //if object.isUri add filter for that particular uri
                    if(object.isURI())
                        newForLoop = new OpFilter(new Expr_Equals(new Const_String(object.getURI()), Var.alloc(AqlUtils.buildVar(forloopVar, ArangoAttributes.VALUE))), newForLoop);

                    //create assignment for storing property uri ie. object with attributes type = URI and value = full rdf:type uri
                    Map<String, Expr> properties_for_property_object = new HashMap<>();
                    properties_for_property_object.put(ArangoAttributes.TYPE, new Const_String(RdfObjectTypes.IRI));
                    properties_for_property_object.put(ArangoAttributes.VALUE, new Const_String(RDF.type.getURI()));
                    //TODO use below property if we need itttt (if a predicate is a bound variable)
                    //properties_for_property_object.put("property_name", new Const_String(ArangoAttributes.RDF_TYPE));
                    AqlQueryNode propertyAssignment = new AssignedResource("full_property_object", new Const_Object(properties_for_property_object));
                    //nest property assignment in for loop
                    newForLoop = new OpNest(newForLoop, propertyAssignment);
                    //create list of projectable vars
                    com.aql.algebra.expressions.VarExprList varExprList = RewritingUtils.CreateProjectionVarExprList(usedVars);
                    varExprList.add(Var.alloc(predicateVar), Var.alloc("full_property_object"));
                    if(object.isVariable())
                        varExprList.add(Var.alloc(object.getName()), Var.alloc(forloopVar));
                    newForLoop = new OpProject(newForLoop, varExprList, false);
                    //save the assignment var into an outer variable for accessing later (need to union it to graph edges result)
                    rdfTypes_assigned_var = assignmentVarGenerator.getNew();
                    currAqlNode = new OpNest(currAqlNode, new AssignedResource(rdfTypes_assigned_var, (Op)newForLoop));
                }
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

            RewritingUtils.ProcessTripleNode(predicate, AqlUtils.buildVar(iterationPathVar, "edges[0]", ArangoAttributes.PREDICATE), filterConditions, usedVars, true);
            RewritingUtils.ProcessTripleNode(object, AqlUtils.buildVar(iterationPathVar, "vertices[1]"), filterConditions, usedVars, false);

            if(filterConditions.size() > 0)
                aqlNode = new com.aql.algebra.operators.OpFilter(filterConditions, aqlNode);

            if(unionToRdfTypes){
                com.aql.algebra.expressions.VarExprList varExprList = RewritingUtils.CreateProjectionVarExprList(usedVars);
                aqlNode = new OpProject(aqlNode, varExprList, false);
                String graphEdgesAssignedVar = assignmentVarGenerator.getNew();
                currAqlNode = new OpNest(currAqlNode, new AssignedResource(graphEdgesAssignedVar, (Op)aqlNode));
                String unionAssignedVar = assignmentVarGenerator.getNew();
                currAqlNode = new OpNest(currAqlNode, new AssignedResource(unionAssignedVar, new Expr_Union(new ExprVar(rdfTypes_assigned_var), new ExprVar(Var.alloc(graphEdgesAssignedVar)))));
                currAqlNode = new OpNest(currAqlNode, new IterationResource(assignmentVarGenerator.getNew(), new ExprVar(unionAssignedVar)));
                currAqlNode = new OpProject(currAqlNode, new ExprVar(assignmentVarGenerator.getCurrent()), false);
            }
            else{
                if(currAqlNode == null) {
                    currAqlNode = aqlNode;
                }
                else {
                    currAqlNode = new OpNest(currAqlNode, aqlNode);
                }
            }

            //firstTripleBeingProcessed = false;
        }

        //add used vars in bgp to list
        SetSparqlVariablesByOp(opBgp.hashCode(), usedVars);
        createdAqlNodes.add(currAqlNode);
    }
}

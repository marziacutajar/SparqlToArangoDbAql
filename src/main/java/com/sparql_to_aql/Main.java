package com.sparql_to_aql;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.AqlQuerySerializer;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.database.ArangoDbClient;
import com.sparql_to_aql.entities.algebra.transformers.OpDistinctTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpGraphTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpProjectOverSliceTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpReducedTransformer;
import org.apache.commons.cli.*;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.algebra.walker.Walker;
import org.apache.jena.sparql.sse.SSE;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Main {

    public enum ARANGODATAMODEL
    {
        D, G
    }

    public static void main(String[] args) {
        // create the parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Path to file containing SPARQL query").argName("file").required().build());
        options.addOption(Option.builder("m").longOpt("data_model").hasArg().desc("ArangoDB data model being queried; Value must be either 'D' if the document model transformation was used, or 'G' if the graph model transformation was used").argName("data model").required().build());

        //initialise ARQ before making any calls to Jena, otherwise running jar file throws exception
        ARQ.init();

        try {

            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            System.out.println("Reading SPARQL query from file...");
            String filePath = line.getOptionValue("f");
            String sparqlQuery = new String(Files.readAllBytes(Paths.get(filePath)));

            ARANGODATAMODEL data_model = ARANGODATAMODEL.valueOf(line.getOptionValue("m"));

            //TODO below QueryFactory.create part is very slow unless it's warmed up! make sure to think about this when measuring performance time
            Query query = QueryFactory.create(sparqlQuery);

            System.out.println("generating algebra");
            Op op = Algebra.compile(query);

            System.out.println("writing algebra");

            SSE.write(op);

            //below is used to get query back to SPARQL query form - create something similar for changing AQL query expression tree into string form
            //Query queryForm = OpAsQuery.asQuery(op);
            //System.out.println(queryForm.serialize());

            System.out.println("initial validation and optimization of algebra");
            //call any optimization transformers on the algebra tree

            //TODO TransformPattern2Join is useful if we want to process all triples seperately instead of as BGPs
            // however not having triples in the same bgp nested in the same subquery will make it slower..I think..
            //op = Transformer.transform(new TransformPattern2Join(), op);

            //transformer to merge project and distinct operators into one
            op = Transformer.transform(new OpDistinctTransformer(), op);

            //transformer to use if we're gonna remove REDUCED from a query and just do a normal project
            op = Transformer.transform(new OpReducedTransformer(), op);

            //transformer to use to combine graph and its nested bgp into one operator
            op = Transformer.transform(new OpGraphTransformer(), op);

            //transformer to use to nest slice op in project op instead of vice versa - IMPORTANT this must be applied before OpDistinctTransformer
            op = Transformer.transform(new OpProjectOverSliceTransformer(), op);

            //TODO consider using below but would have to add support for OpSequence
            //op = Transformer.transform(new TransformFilterPlacement(), op);
            //consider also these existing transformers:
            //TransformExtendCombine, TransformFilterEquality, TransformFilterInequality, TransformRemoveAssignment, TransformImplicitLeftJoin, TransformFilterPlacement, TransformMergeBGPs
            SSE.write(op);

            //get FROM and FROM NAMED uris
            List<String> namedGraphs = query.getNamedGraphURIs(); //get all FROM NAMED uris
            List<String> defaultGraphUris = query.getGraphURIs(); //get all FROM uris (forming default graph)

            ArqToAqlAlgebraVisitor queryExpressionTranslator;

            switch (data_model){
                case D:
                    queryExpressionTranslator = new ArqToAqlAlgebraVisitor_DocVersion(defaultGraphUris, namedGraphs);
                    break;
                case G:
                    queryExpressionTranslator = new ArqToAqlAlgebraVisitor_GraphVersion(defaultGraphUris, namedGraphs);
                    break;
                default: throw new RuntimeException("Unsupported ArangoDB data model");
            }

            OpWalker.walk(op, queryExpressionTranslator);

            //Use AQL query serializer to get actual AQL query
            StringWriter out = new StringWriter();
            AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(out);
            //AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(System.out);

            List<AqlQueryNode> aqlQueryExpressionSubParts = queryExpressionTranslator.GetAqlAlgebraQueryExpression();
            for(AqlQueryNode aqlQueryPart: aqlQueryExpressionSubParts){
                //TODO this might be an issue... best to use OpSequence and iterate inside the serializer instead..
                // or call another method here that tells serializer to just add text to current query with correct indents...
                aqlQueryPart.visit(aqlQuerySerializer);
                aqlQuerySerializer.finishVisit();
            }

            String aqlQuery = out.toString();
            System.out.println(aqlQuery);

            //TODO also consider using before and after visitors if we need them... we might
            //TODO possibly use below tutorial for visitor pattern to translate algebra tree
            //https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P

            System.out.println("execute generated AQL query on ArangoDb");
            ArangoCursor<BaseDocument> results = new ArangoDbClient().execQuery(ArangoDatabaseSettings.databaseName, aqlQuery);
            //results.asListRemaining().forEach(r -> System.out.println(r));
            while(results.hasNext()){
                BaseDocument curr = results.next();
                Map<String, Object> rowColumns = curr.getProperties();
                Iterator<Map.Entry<String, Object>> it = rowColumns.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Object> pair = it.next();

                    if(pair.getValue() instanceof HashMap){
                        Map<String, Object> values = ((HashMap) pair.getValue());
                        String type = values.get(ArangoAttributes.TYPE).toString();
                        switch (type){
                            case RdfObjectTypes.IRI:
                                //TODO get value and format it as uri
                                break;
                            case RdfObjectTypes.LITERAL:
                                //TODO get value and format it if necessary (quoted if string, just value if otherwise), if it has lang put @lang as well
                                break;
                            default:
                                throw new UnsupportedOperationException("Type not supported");
                        }
                    }
                    else{
                        //TODO just output value as is?
                    }
                }
            }
        }
        catch(IOException e){
            System.out.println("File not found.");
        }
        catch(QueryException qe){
            System.out.println("Invalid SPARQL query.");
        }
        catch(ParseException exp) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        System.exit(0);
    }
}

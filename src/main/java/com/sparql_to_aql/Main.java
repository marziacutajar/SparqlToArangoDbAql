package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.AqlQuerySerializer;
import com.aql.algebra.AqlAlgebraTreeWriter;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.database.ArangoDbClient;
import com.sparql_to_aql.entities.algebra.transformers.OpDistinctTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpGraphTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpProjectOverSliceTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpReducedTransformer;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.SparqlUtils;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.sse.SSE;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Main {

    private static final int queryRuns = 15;

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
            //TODO consider changing option f to accept more than one file path so we can translate, run, and measure the running time of all the queries with one call
            String filePath = line.getOptionValue("f");
            String fileName = FilenameUtils.removeExtension(new File(filePath).getName());

            String sparqlQuery = new String(Files.readAllBytes(Paths.get(filePath)));

            ArangoDataModel data_model = ArangoDataModel.valueOf(line.getOptionValue("m"));
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
                    queryExpressionTranslator = new ArqToAqlAlgebraVisitor_BasicApproach(defaultGraphUris, namedGraphs);
                    break;
                case G:
                    queryExpressionTranslator = new ArqToAqlAlgebraVisitor_GraphApproach(defaultGraphUris, namedGraphs);
                    break;
                default: throw new RuntimeException("Unsupported ArangoDB data model");
            }

            OpWalker.walk(op, queryExpressionTranslator);

            AqlQueryNode aqlQueryExpression = queryExpressionTranslator.GetAqlAlgebraQueryExpression();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            AqlAlgebraTreeWriter aqlAlgebraWriter = new AqlAlgebraTreeWriter(outputStream);
            aqlQueryExpression.visit(aqlAlgebraWriter);
            aqlAlgebraWriter.finishVisit();

            System.out.println(outputStream.toString());
            outputStream.close();

            //Use AQL query serializer to get actual AQL query
            StringWriter out = new StringWriter();
            AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(out);
            //AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(System.out);
            aqlQueryExpression.visit(aqlQuerySerializer);
            aqlQuerySerializer.finishVisit();

            String aqlQuery = out.toString();
            System.out.println(aqlQuery);

            ArangoDbClient arangoDbClient = new ArangoDbClient();
            ArangoCursor<BaseDocument> results = null;
            System.out.println("execute generated AQL query on ArangoDb");

            long[] timeMeasurements = new long[queryRuns];
            //long sum = 0;
            String directoryName = "runtime_results/" + data_model.toString();
            new File(directoryName).mkdirs();

            FileWriter csvWriter = new FileWriter( directoryName + "/" + fileName + ".csv");

            for(int i = 0; i < queryRuns; i++) {
                Instant start = Instant.now();
                results = arangoDbClient.execQuery(ArangoDatabaseSettings.databaseName, aqlQuery);
                Instant finish = Instant.now();
                timeMeasurements[i] = Duration.between(start, finish).toMillis();
                //sum += timeMeasurements[i];
                csvWriter.append(String.valueOf(timeMeasurements[i]));
                if(i < queryRuns-1){
                    csvWriter.append(",");
                }
            }

            csvWriter.flush();
            csvWriter.close();

            /*long averageRuntime = (sum / queryRuns);
            System.out.println("Average query runtime: " + averageRuntime + "ms");*/

            while(results.hasNext()){
                BaseDocument curr = results.next();
                Map<String, Object> rowColumns = curr.getProperties();
                Iterator<Map.Entry<String, Object>> it = rowColumns.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Object> pair = it.next();
                    String variableName = pair.getKey();
                    if(pair.getValue() instanceof HashMap){
                        Map<String, Object> values = ((HashMap) pair.getValue());
                        String formattedValue;
                        String type = values.get(ArangoAttributes.TYPE).toString();
                        //get value and format it if necessary (quoted if string, just value if otherwise), if it has lang put @lang as well
                        switch (type){
                            case RdfObjectTypes.IRI:
                                //TODO stop using SparqlUtils.formatIri.. IRIs are only surrounded by square brackets in TURTLE
                                formattedValue = SparqlUtils.formatIri(values.get(ArangoAttributes.VALUE).toString());
                                break;
                            case RdfObjectTypes.LITERAL:
                                String val = values.get(ArangoAttributes.VALUE).toString();
                                if(values.get(ArangoAttributes.VALUE) instanceof String){
                                    formattedValue = AqlUtils.quoteString(val);
                                    if(values.get(ArangoAttributes.LITERAL_LANGUAGE) != null){
                                        formattedValue += "@" + values.get(ArangoAttributes.LITERAL_LANGUAGE).toString();
                                    }
                                }
                                else{
                                    formattedValue = val;
                                }

                                //String datatype = values.get(ArangoAttributes.LITERAL_DATA_TYPE).toString();
                                //System.out.println(variableName + ": " + formattedValue);
                                break;
                            default:
                                throw new UnsupportedOperationException("Type not supported");
                        }
                    }
                    else{
                        //TODO check what type the value is, ex. int, bool etc. and output the value as it would be in SPARQL
                        System.out.print(pair.getValue());
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

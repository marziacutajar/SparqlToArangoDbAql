package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.AqlQuerySerializer;
import com.aql.algebra.AqlAlgebraTreeWriter;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.database.ArangoDbClient;
import com.sparql_to_aql.entities.algebra.transformers.OpDistinctTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpGraphTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpProjectOverSliceTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpReducedTransformer;
import com.sparql_to_aql.utils.AqlUtils;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.sse.SSE;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class Main {

    private static final int queryRuns = 15;
    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    public static void main(String[] args) {
        // create the parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        //accept more than one file path so we can translate, run, and measure the running time of multiple queries with one call
        options.addOption(Option.builder("f").longOpt("files").hasArgs().desc("Paths to one or more files containing a SPARQL query").argName("file").required().build());
        options.addOption(Option.builder("m").longOpt("data_model").hasArg().desc("ArangoDB data model being queried; Value must be either 'D' if the document model transformation was used, or 'G' if the graph model transformation was used").argName("data model").required().build());

        //initialise ARQ before making any calls to Jena, otherwise running jar file throws exception
        ARQ.init();

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            ArangoDataModel data_model = ArangoDataModel.valueOf(line.getOptionValue("m"));
            String[] filePaths = line.getOptionValues("f");

            String directoryName = "runtime_results/" + data_model.toString();
            new File(directoryName).mkdirs();

            String formattedDate = DATE_FORMAT.format(new Date());
            //add results of multiple queries to the same results file (one row for each query, make first column the query file name)
            FileWriter csvWriter = new FileWriter(directoryName + "/" + formattedDate + ".csv");

            List<File> files = GetFilesFromArgumentPaths(filePaths);

            for(File f: files) {
                String filePath = f.getPath();
                //catch exceptions per file, so if one file has an issue we skip it and go to the next
                try {
                    String fileName = FilenameUtils.removeExtension(f.getName());

                    System.out.println("Processing SPARQL query from file " + filePath);
                    String sparqlQuery = new String(Files.readAllBytes(Paths.get(filePath)));

                    //below QueryFactory.create call is very slow unless it's warmed up! make sure to think about this when measuring performance time
                    Query query = QueryFactory.create(sparqlQuery);

                    String aqlQuery = SparqlQueryToAqlQuery(query, data_model);
                    //System.out.println(aqlQuery);

                    ArangoDbClient arangoDbClient = new ArangoDbClient();
                    ArangoCursor<BaseDocument> results = null;
                    System.out.println("Executing generated AQL query on ArangoDb..");

                    long[] timeMeasurements = new long[queryRuns];
                    //long sum = 0;

                    csvWriter.append(fileName + ",");
                    for (int i = 0; i < queryRuns; i++) {
                        Instant start = Instant.now();
                        results = arangoDbClient.execQuery(ArangoDatabaseSettings.databaseName, aqlQuery);
                        Instant finish = Instant.now();
                        timeMeasurements[i] = Duration.between(start, finish).toMillis();
                        //sum += timeMeasurements[i];
                        csvWriter.append(String.valueOf(timeMeasurements[i]));
                        if (i < queryRuns - 1) {
                            csvWriter.append(",");
                        }
                    }

                    csvWriter.append("\r\n");
                    csvWriter.flush();

                    /*long averageRuntime = (sum / queryRuns);
                    System.out.println("Average query runtime: " + averageRuntime + "ms");*/
                    AqlUtils.printQueryResultsAsSparql(results);
                }
                catch (IOException e) {
                    System.out.println("File not found: " + filePath);
                }
                catch (QueryException qe) {
                    System.out.println("Invalid SPARQL query. ");
                }
                catch(UnsupportedOperationException e){
                    System.out.println(e);
                }
            }

            csvWriter.close();
        }
        catch(IOException e){
            System.out.println("Error reading/writing file.");
        }
        catch(ParseException exp) {
            System.err.println("Parsing failed. Reason: " + exp.getMessage());
        }

        System.exit(0);
    }

    public static String SparqlQueryToAqlQuery(Query query, ArangoDataModel dataModel) throws IOException {
        //System.out.println("Generating algebra");
        Op op = Algebra.compile(query);

        //System.out.println("Writing algebra");
        //SSE.write(op);

        //below is used to get query back to SPARQL query form - create something similar for changing AQL query expression tree into string form
        //Query queryForm = OpAsQuery.asQuery(op);
        //System.out.println(queryForm.serialize());

        //System.out.println("initial validation and optimization of algebra");
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
        //SSE.write(op);

        //get FROM and FROM NAMED uris
        List<String> namedGraphs = query.getNamedGraphURIs(); //get all FROM NAMED uris
        List<String> defaultGraphUris = query.getGraphURIs(); //get all FROM uris (forming default graph)

        ArqToAqlAlgebraVisitor queryExpressionTranslator;

        switch (dataModel){
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

        //System.out.println(outputStream.toString());
        outputStream.close();

        //Use AQL query serializer to get actual AQL query
        StringWriter out = new StringWriter();
        AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(out);
        //AqlQuerySerializer aqlQuerySerializer = new AqlQuerySerializer(System.out);
        aqlQueryExpression.visit(aqlQuerySerializer);
        aqlQuerySerializer.finishVisit();

        return out.toString();
    }

    public static List<File> GetFilesFromArgumentPaths(String[] filePaths){
        List<File> files = new ArrayList<>();

        for (String filePath: filePaths) {
            File file = new File(filePath);
            if (file.isDirectory()) {
                //get list of all files in directory and we'll process them all
                for (File fObj : file.listFiles()) {
                    if (fObj.isFile())
                        files.add(fObj);
                }
            } else {
                files.add(file);
            }
        }

        return files;
    }
}

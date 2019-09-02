package com.sparql_to_aql;

import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.database.ArangoDbClient;
import com.sparql_to_aql.entities.algebra.transformers.OpDistinctTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpGraphTransformer;
import com.sparql_to_aql.entities.algebra.transformers.OpReducedTransformer;
import org.apache.commons.cli.*;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.sse.SSE;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) {
        // create the parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Path to file containing SPARQL query").argName("file").required().build());

        //initialise ARQ before making any calls to Jena, otherwise running jar file throws exception
        ARQ.init();

        try {

            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            System.out.println("Reading SPARQL query from file...");
            String filePath = line.getOptionValue("f");
            String sparqlQuery = new String(Files.readAllBytes(Paths.get(filePath)));

            //TODO below QueryFactory.create part is very slow unless it's warmed up! make sure to think about this when measuring performance time
            Query query = QueryFactory.create(sparqlQuery);

            System.out.println("getting graphs");

            //testing how to get FROM and FROM NAMED uris
            //query.getNamedGraphURIs().forEach(f-> System.out.println(f)); //get all FROM NAMED uris
            //query.getGraphURIs().forEach(f-> System.out.println(f)); //get all FROM uris (forming default graph)

            System.out.println("generating algebra");
            Op op = Algebra.compile(query);

            System.out.println("writing algebra");

            SSE.write(op);

            //TODO below is used to get query back to SPARQL query form - create something similar for changing AQL query expression tree into string form
            //Query queryForm = OpAsQuery.asQuery(op);
            //System.out.println(queryForm.serialize());

            System.out.println("initial validation and optimization of algebra");
            //call any optimization transformers on the algebra tree
            //op = Algebra.optimize(op);
            //op = Transformer.transform(new TransformTopN(), op);
            //TODO consider using below quad form.. might be easier to parse than having a GRAPH operator..
            //op = Algebra.toQuadForm(op);
            //TODO TransformPattern2Join is useful if we want to process all triples seperately instead of as BGPs
            // however not having triples in the same bgp nested in the same subquery will make it slower..I think..
            //op = Transformer.transform(new TransformPattern2Join(), op);
            op = Transformer.transform(new OpDistinctTransformer(), op);

            //transformer to use if we're gonna remove REDUCED from a query and just do a normal project
            op = Transformer.transform(new OpReducedTransformer(), op);

            //transformer to use to combine graph and its nested bgp into one operator
            op = Transformer.transform(new OpGraphTransformer(), op);

            //TODO consider also these existing transformers:
            //TransformExtendCombine, TransformFilterEquality, TransformFilterInequality, TransformRemoveAssignment
            SSE.write(op);
            OpWalker.walk(op, new ArqToAqlAlgebraVisitor(query.getGraphURIs(), query.getNamedGraphURIs()));

            //OpWalker.walk(op, new RewritingOpVisitor());
            //TODO use below walker once we're also using the expression walker.. or might have to extend WalkerVisitor instead..
            //Walker.walk(op, new WalkerVisitor(new RewritingOpVisitor(), new RewritingExprVisitor(), null, null));
            //TODO also consider using before and after visitors if we need them... we might
            //TODO possibly use below tutorial for visitor pattern to translate algebra tree
            //https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P

            System.out.println("execute generated AQL query on ArangoDb");
            //TODO refer to https://www.arangodb.com/tutorials/tutorial-sync-java-driver/
            //new ArangoDbClient().execQuery(ArangoDatabaseSettings.databaseName, aqlQuery_here);
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
    }
}

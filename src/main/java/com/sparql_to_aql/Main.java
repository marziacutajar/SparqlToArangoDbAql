package com.sparql_to_aql;

import com.sparql_to_aql.database.ArangoDbClient;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.jena.base.Sys;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.sse.SSE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

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
            query.getNamedGraphURIs().forEach(f-> System.out.println(f)); //get all FROM NAMED uris
            query.getGraphURIs().forEach(f-> System.out.println(f)); //get all FROM uris (forming default graph)

            System.out.println("generating algebra");

            Op op = Algebra.compile(query);

            System.out.println("writing algebra");

            SSE.write(op);

            System.out.println("initial validation and optimization of algebra");
            //TODO
            //op = Algebra.optimize(op);
            //OpWalker.walk(op, new SparqlOptimizationVisitor());

            //TODO possibly use below tutorial for visitor pattern to translate algebra tree
            //https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P

            System.out.println("execute generated AQL query on ArangoDb");
            //TODO refer to https://www.arangodb.com/tutorials/tutorial-sync-java-driver/
            //new ArangoDbClient().execQuery(dbname_here, query_here);
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

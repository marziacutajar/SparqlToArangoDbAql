package com.sparql_to_aql.utils;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class SparqlUtils {
    public static String formatIri(String iri){
        return "<" + iri + ">";
    }

    public static void printSparqlQueryResultsToFile(ResultSet results, String filePath) throws IOException {
        FileWriter csvWriter = new FileWriter(filePath);
        List<String> resultVars = results.getResultVars();
        for (String projectedVar: resultVars) {
            csvWriter.append(projectedVar + ",");
        }
        csvWriter.append("\r\n");

        while(results.hasNext()) {
            QuerySolution curr = results.next();
            for (String projectedVar: resultVars) {
                csvWriter.append(curr.get(projectedVar) + ",");
            }
            csvWriter.append("\r\n");
        }

        csvWriter.flush();
        csvWriter.close();
    }
}

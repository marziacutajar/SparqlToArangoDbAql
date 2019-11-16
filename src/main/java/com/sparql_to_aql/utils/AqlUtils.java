package com.sparql_to_aql.utils;

public class AqlUtils {
    public static String quoteString(String string){
        return "\"" + string + "\"";
    }

    //build a variable for a property nested in some JSON object
    public static String buildVar(String... varParts) {
        String separator = ".";

        //TODO if any item in varParts is null or empty string/whitespace, remove it/them
        return String.join(separator, varParts);
    }

    public static String escapeString(String string) { return "`" + string + "`";}
}

package com.sparql_to_aql.utils;

public class AqlUtils {
    public static String quoteString(String string){
        return "\"" + string + "\"";
    }

    public static String buildVar(String... varParts) {
        String separator = ".";

        return String.join(separator, varParts);
    }
}

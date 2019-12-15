package com.sparql_to_aql.utils;

import java.util.Arrays;
import java.util.List;

public class AqlUtils {
    public static String quoteString(String string){
        return "\"" + string + "\"";
    }

    //build a variable for a property nested in some JSON object
    public static String buildVar(String... varParts) {
        String separator = ".";

        //if any item in varParts is null or empty string/whitespace, remove it/them
        List<String> extrasList = Arrays.asList(varParts);
        extrasList.removeIf(s -> s == null || s.isEmpty());

        return String.join(separator, extrasList);
    }

    public static String escapeString(String string) { return "`" + string + "`";}
}

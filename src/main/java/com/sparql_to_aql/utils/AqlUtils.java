package com.sparql_to_aql.utils;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.RdfObjectTypes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AqlUtils {
    public static String quoteString(String string){
        return "\"" + string + "\"";
    }

    //build a variable for a property nested in some JSON object
    public static String buildVar(String... varParts) {
        String separator = ".";

        return String.join(separator, varParts);
    }

    public static String escapeString(String string) { return "`" + string + "`";}

    public static void printQueryResultsAsSparql(ArangoCursor<BaseDocument> results){
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
                            formattedValue = values.get(ArangoAttributes.VALUE).toString();
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
                    Object val = pair.getValue();
                    if(val == null)
                        System.out.print("");
                    else
                        System.out.print(pair.getValue());
                }
            }
        }
    }
}

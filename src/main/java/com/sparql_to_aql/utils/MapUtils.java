package com.sparql_to_aql.utils;

import java.util.Map;
import java.util.Set;

public class MapUtils{

    public static Set<String> GetCommonMapKeys(Map<String, String> map1, Map<String, String> map2){
        Set<String> commonVars = map1.keySet();

        commonVars.retainAll(map2.keySet());

        return commonVars;
    }

    public static Map<String, String> MergeMapsKeepFirstDuplicateKeyValue(Map<String, String> map1, Map<String, String> map2){
        //keep only the value in the first map for duplicate keys
        map2.keySet().removeAll(map1.keySet());
        map1.putAll(map2);
        return map1;
    }
}

package com.sparql_to_aql.utils;

import org.apache.commons.lang3.SerializationUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapUtils{

    /**
     * Return the set of common keys between two maps
     * @param map1
     * @param map2
     * @return set of common keys between map1 and map2
     */
    public static Set<String> GetCommonMapKeys(Map<String, String> map1, Map<String, String> map2){
        Set<String> commonVars = new HashSet<>(map1.keySet());

        commonVars.retainAll(map2.keySet());

        return commonVars;
    }

    /**
     * Merge two maps into one
     * @param map1
     * @param map2
     * @return merged map
     */
    public static Map<String, String> MergeMapsKeepFirstDuplicateKeyValue(Map<String, String> map1, Map<String, String> map2){
        //keep only the value in the first map for duplicate keys
        map2.keySet().removeAll(map1.keySet());
        map1.putAll(map2);
        return map1;
    }
}

package com.sparql_to_aql.utils;

import com.sparql_to_aql.entities.BoundAqlVarDetails;
import com.sparql_to_aql.entities.BoundAqlVars;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;
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
    public static Set<String> GetCommonMapKeys(Map<String, BoundAqlVars> map1, Map<String, BoundAqlVars> map2){
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
    public static Map<String, BoundAqlVars> MergeBoundAqlVarsMaps(Map<String, BoundAqlVars> map1, Map<String, BoundAqlVars> map2){
        Map<String, BoundAqlVars> newMap = new HashMap<>();
        newMap.putAll(map1);

        for (Map.Entry<String, BoundAqlVars> entry : map2.entrySet()) {
            String sparqlVarName = entry.getKey();
            if(newMap.containsKey(sparqlVarName)){
                //if value in left map (map1) can't be null then ignore the value in right map (map2).. otherwise add the aql vars in map2 to the possible ones in map1
                if(newMap.get(sparqlVarName).canBeNull()){
                    newMap.get(sparqlVarName).addVars(entry.getValue().getVars());
                }
            }
            else{
                newMap.put(sparqlVarName, entry.getValue());
            }
        }

        return newMap;
    }
}

package com.sparql_to_aql.utils;

import java.util.Collections;
import java.util.List;

public class MathUtils {
    public static long calculateAverageLong(List<Long> measurements, int numOutliersToRemove){
        Collections.sort(measurements);
        List<Long> modifiedList = measurements.subList(numOutliersToRemove, measurements.size() - 1 - numOutliersToRemove);
        Long sum = modifiedList.stream().mapToLong(Long::longValue).sum();
        return (sum / modifiedList.size());
    }

    public static double calculateAverageDouble(List<Double> measurements, int numOutliersToRemove){
        Collections.sort(measurements);
        List<Double> modifiedList = measurements.subList(numOutliersToRemove, measurements.size() - 1 - numOutliersToRemove);
        Double sum = modifiedList.stream().mapToDouble(Double::doubleValue).sum();
        return (sum / modifiedList.size());
    }
}

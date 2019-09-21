package com.sparql_to_aql.constants;

public class ArangoDatabaseSettings {
    public static final String databaseName = "_system";

    public static class DocumentModel{
        public static final String rdfCollectionName = "triples";
    }

    public static class GraphModel{
        public static final String rdfCollectionName = "vertices";
        public static final String rdfEdgeCollectionName = "edges";
    }
}

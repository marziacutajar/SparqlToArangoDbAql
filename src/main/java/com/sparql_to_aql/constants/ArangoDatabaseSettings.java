package com.sparql_to_aql.constants;

//TODO put below in config.properties file
public class ArangoDatabaseSettings {
    public static final String databaseName = "_system";

    public static class DocumentModel{
        public static final String rdfCollectionName = "triples";
    }

    public static class GraphModel{
        public static final String rdfResourcesCollectionName = "vertices_resources";
        public static final String rdfLiteralsCollectionName = "vertices_literals";
        public static final String rdfEdgeCollectionName = "edges";
    }
}

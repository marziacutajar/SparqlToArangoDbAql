package com.sparql_to_aql.constants;

public class ArangoDatabaseSettings {
    public static class DocumentModel{
        public static final String rdfCollectionName = Configuration.GetDocModelCollectionName();
    }

    public static class GraphModel{
        public static final String rdfResourcesCollectionName = Configuration.GetGraphModelResourcesCollectionName();
        public static final String rdfLiteralsCollectionName = Configuration.GetGraphModelLiteralsCollectionName();
        public static final String rdfEdgeCollectionName = Configuration.GetGraphModelEdgesCollectionName();
    }
}

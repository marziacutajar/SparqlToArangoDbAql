package com.sparql_to_aql.constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.util.Properties;

public class Configuration {
    private static final String NAME = "config.properties";
    public static final Properties properties;

    public static class Keys {
        public static final String ARANGO_DATABASE_NAME = "arangodb.databaseName";
        public static final String ARANGO_DOCMODEL_COLLECTION_NAME = "arangodb.documentModel.collectionName";
        public static final String ARANGO_GRAPHMODEL_RESOURCES_COLLECTION_NAME = "arangodb.graphModel.resourcesCollectionName";
        public static final String ARANGO_GRAPHMODEL_LITERALS_COLLECTION_NAME = "arangodb.graphModel.literalsCollectionName";
        public static final String ARANGO_GRAPHMODEL_EDGES_COLLECTION_NAME = "arangodb.graphModel.edgeCollectionName";
        public static final String VIRTUOSO_URL = "virtuoso.url";
        public static final String VIRTUOSO_GRAPH_NAME = "virtuoso.graphName";
        public static final String VIRTUOSO_USER = "virtuoso.user";
        public static final String VIRTUOSO_PASSWORD = "virtuoso.password";
    }

    static {
        Properties fallback = new Properties();
        fallback.put(Keys.ARANGO_DATABASE_NAME, "_system");
        fallback.put(Keys.ARANGO_DOCMODEL_COLLECTION_NAME, "triples");
        fallback.put(Keys.ARANGO_GRAPHMODEL_RESOURCES_COLLECTION_NAME, "vertices_resources");
        fallback.put(Keys.ARANGO_GRAPHMODEL_LITERALS_COLLECTION_NAME, "vertices_literals");
        fallback.put(Keys.ARANGO_GRAPHMODEL_EDGES_COLLECTION_NAME, "edges");
        fallback.put(Keys.VIRTUOSO_URL, "jdbc:virtuoso://localhost:1111");
        fallback.put(Keys.VIRTUOSO_USER, "dba");
        fallback.put(Keys.VIRTUOSO_PASSWORD, "dba");

        properties = new Properties(fallback);

        try (InputStream input = new FileInputStream(NAME)) {
            // load a properties file
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String GetArangoDatabaseName(){
        return properties.getProperty(Keys.ARANGO_DATABASE_NAME);
    }

    public static String GetDocModelCollectionName(){
        return properties.getProperty(Keys.ARANGO_DOCMODEL_COLLECTION_NAME);
    }

    public static String GetGraphModelResourcesCollectionName(){
        return properties.getProperty(Keys.ARANGO_GRAPHMODEL_RESOURCES_COLLECTION_NAME);
    }

    public static String GetGraphModelLiteralsCollectionName(){
        return properties.getProperty(Keys.ARANGO_GRAPHMODEL_LITERALS_COLLECTION_NAME);
    }

    public static String GetGraphModelEdgesCollectionName(){
        return properties.getProperty(Keys.ARANGO_GRAPHMODEL_EDGES_COLLECTION_NAME);
    }

    public static String GetVirtuosoUrl(){
        return properties.getProperty(Keys.VIRTUOSO_URL);
    }

    public static String GetVirtuosoGraphName(){
        return properties.getProperty(Keys.VIRTUOSO_GRAPH_NAME);
    }

    public static String GetVirtuosoUser(){
        return properties.getProperty(Keys.VIRTUOSO_USER);
    }

    public static String GetVirtuosoPassword(){
        return properties.getProperty(Keys.VIRTUOSO_PASSWORD);
    }
}

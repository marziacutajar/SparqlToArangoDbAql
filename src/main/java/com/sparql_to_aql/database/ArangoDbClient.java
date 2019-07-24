package com.sparql_to_aql.database;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.internal.ArangoDefaults;
import com.arangodb.model.AqlQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoDbClient {
    //logger for logging exceptions
    //private static Logger logger = LoggerFactory.getLogger(ArangoDbClient.class);

    //TODO actually make it singleton..
    /** ArangoDB connection, singleton. */
    private static ArangoDB arangoDbConnection;

    public ArangoDbClient(){
        arangoDbConnection = new ArangoDB.Builder().build();
    }

    public ArangoCursor<BaseDocument> execQuery(String dbName, String query){
        //try {
            return arangoDbConnection.db(dbName).query(query, null, null,
                    BaseDocument.class);
        //}
        //catch (final ArangoDBException e){
        //    //TODO throw custom exception or something??
        //}
    }
}

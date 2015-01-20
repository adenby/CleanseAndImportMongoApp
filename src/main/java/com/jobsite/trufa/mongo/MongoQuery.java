package com.jobsite.trufa.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class MongoQuery {

	public void doQuery(DBCollection collection) {
		
//        BasicDBObject query = new BasicDBObject("number", new BasicDBObject("$gt", 45));
//        BasicDBObject query = new BasicDBObject("number", new BasicDBObject("$gt", 45));
        
        DBCursor cursor = collection.find();
//        DBCursor cursor = collection.find(query);
        try {
           while(cursor.hasNext()) {
               System.out.println(cursor.next());
           }
        } finally {
           cursor.close();
        }

	}
}

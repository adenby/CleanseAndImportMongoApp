package com.jobsite.trufa.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongoInsert {
	
	private String SOURCE_KEY = "TRUFA_SOURCE";

	public void doInsert(DBCollection collection, String json, String sourceKey) {
			 
				DBObject dbObject = (DBObject)JSON.parse(json);
				dbObject.put(SOURCE_KEY, sourceKey);
			 
				collection.insert(dbObject);		
	}	
}

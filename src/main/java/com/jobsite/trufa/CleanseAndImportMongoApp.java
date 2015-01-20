package com.jobsite.trufa;

import java.io.File;
import java.io.FilenameFilter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.jobsite.trufa.fileReaders.ScrapinghubFileReader;
import com.jobsite.trufa.fileReaders.SwanFileReader;
import com.jobsite.trufa.mongo.MongoInsert;
import com.jobsite.trufa.util.Logger;
import com.jobsite.trufa.util.StatusWriter;
import com.jobsite.trufa.util.DirectoryScanner;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class CleanseAndImportMongoApp 
{
	private static File SWAN_DATA_DIRECTORY = new File("C:/Workspaces/jobsite/trufa/Common-Data/Swan");
	private static File SCRAPINGHUB_DATA_DIRECTORY = new File("C:/Workspaces/jobsite/trufa/Common-Data/ScrapingHub");
	
	private static final String _SWAN_ONLY_ARG = "SwanOnly";
	private static final String _SCRAPINGHUB_ONLY_ARG = "ScrapingHubOnly";
	private static final String _SWAN_DATA_DIRECTORY_ARG = "SwanDataDirectory";
	private static final String _SCRAPINGHUB_DATA_DIRECTORY_ARG = "ScrapingHubDataDirectory";

	private static boolean _swan_only = false; 
	private static boolean _scrapinghub_only = false; 
	private static File _swan_data_directory = SWAN_DATA_DIRECTORY; 
	private static File _scrapinghub_data_directory = SCRAPINGHUB_DATA_DIRECTORY; 

	private static boolean _import_swan = true; 
	private static boolean _import_scrapinghub = true; 

	private static SimpleDateFormat _simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

	private static void readArgs(String[] args) {
		for(int i=0;i<args.length;++i) 
		{
			String arg = args[i];
		    if (arg.toLowerCase().startsWith(_SWAN_ONLY_ARG.toLowerCase())) {
		    	_swan_only = true;
		    } else if (arg.toLowerCase().startsWith(_SCRAPINGHUB_ONLY_ARG.toLowerCase())) {
		    	_scrapinghub_only = true;
		    } else if (arg.toLowerCase().startsWith(_SWAN_DATA_DIRECTORY_ARG.toLowerCase())) {
		    	_swan_data_directory = new File(arg.substring(arg.indexOf("=")+1));
		    } else if (arg.toLowerCase().startsWith(_SCRAPINGHUB_DATA_DIRECTORY_ARG.toLowerCase())) {
		    	_scrapinghub_data_directory = new File(arg.substring(arg.indexOf("=")+1));
		    }
		}		
	}

	private static void printArgs() {
		System.out.println(String.format("\t%s=%b%n\t%s=%b%n\t%s=%s%n\t%s=%s%n", 
			_SWAN_ONLY_ARG, _swan_only,
			_SCRAPINGHUB_ONLY_ARG, _scrapinghub_only,
			_SWAN_DATA_DIRECTORY_ARG, _swan_data_directory,
			_SCRAPINGHUB_DATA_DIRECTORY_ARG, _scrapinghub_data_directory
	    ));
	}
	
    public static void main( String[] args )
    {
        readArgs(args);
        printArgs();

		if (_swan_only && _scrapinghub_only) {
	        System.out.println( String.format("Can't set both %s and %s flags. Please remove one before continuing.", _SWAN_ONLY_ARG, _SCRAPINGHUB_ONLY_ARG));
	        System.exit(0);
		}
		
		if (_swan_only) {
			_import_scrapinghub = false;
		}

		if (_scrapinghub_only) {
			_import_swan = false;
		}

		//check if data folders exist		
		if (_import_swan && !_swan_data_directory.exists()) { 
	        System.out.println( String.format("%s arg specifies a folder that doesn't exist. Please correct before continuing.", _SWAN_DATA_DIRECTORY_ARG));
	        System.exit(0);
		}

		if (_import_scrapinghub && !_scrapinghub_data_directory.exists()) { 
	        System.out.println( String.format("%s arg specifies a folder that doesn't exist. Please correct before continuing.", _SCRAPINGHUB_DATA_DIRECTORY_ARG));
	        System.exit(0);
		}

//		dropCollection("trufa");
		
        if (_import_scrapinghub) {
            System.out.println( "Loading Scrapinghub data into Mongo" );
        	importScrapinghub();
        }
        
        if (_import_swan) {
            System.out.println( "Loading Swan data into Mongo" );
        	importSwan();
        }

        System.exit(0);        
    }

    private static void importScrapinghub() {
        StatusWriter statusWriter = StatusWriter.getInstance();        

        MongoInsert mongoInsert = new MongoInsert();

        MongoClient mongoClient;
		try {
			mongoClient = new MongoClient( "localhost" );
  
			dropCollection("scrapinghub");
			
	        DB db = mongoClient.getDB( "trufa" );
	        
	        DBCollection collection = db.getCollection("scrapinghub");
//	        collection.drop();

	        DirectoryScanner directoryScanner = new DirectoryScanner(_scrapinghub_data_directory, new FilenameFilter() {
//			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".jl");
			    }
			});
	        
//	        Logger debugLog = new Logger(new File("cleansed_json.jl"));

	        while(directoryScanner.hasNext()) {
	        	
	        	int iProcessLineCount = 0;

	        	File jlFile = directoryScanner.next();

	        	printTime();
	        	
	        	ScrapinghubFileReader scrapinghubFileReader = new ScrapinghubFileReader(jlFile);
	        	
	        	while(scrapinghubFileReader.hasNext()) {
	        		String json = scrapinghubFileReader.next();

//	        		debugLog.print(json);
			        mongoInsert.doInsert(collection, json, "ScrapingHub");

			        if (iProcessLineCount > 0 && iProcessLineCount % 25000 == 0) {
			        	statusWriter.writeMessage(String.format("Processed %d lines", iProcessLineCount));
			        }

					iProcessLineCount++;
	        	}

	        	statusWriter.writeMessage(String.format("Processed %d lines\n", iProcessLineCount));

	        }

        	printTime();

//        	debugLog.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
    }

    private static void importSwan() {
        StatusWriter statusWriter = StatusWriter.getInstance();        

        MongoInsert mongoInsert = new MongoInsert();

        MongoClient mongoClient;
		try {
			mongoClient = new MongoClient( "localhost" );
        
	        DB db = mongoClient.getDB( "trufa" );
	        
	        DBCollection collection = db.getCollection("trufa");
//	        collection.drop();

	        DirectoryScanner directoryScanner = new DirectoryScanner(_swan_data_directory, new FilenameFilter() {
//			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".csv");
			    }
			});

	        while(directoryScanner.hasNext()) {
	        	
	        	int iProcessLineCount = 0;

	        	File file = directoryScanner.next();

	        	printTime();
	        	
	        	SwanFileReader swanFileReader = new SwanFileReader(file);
	        	
	        	while(swanFileReader.hasNext()) {
	        		String json = swanFileReader.next();

			        mongoInsert.doInsert(collection, json, "Swan");
			        
			        if (iProcessLineCount > 0 && iProcessLineCount % 100 == 0) {
			        	statusWriter.writeMessage(String.format("Processed %d lines", iProcessLineCount));
			        }

					iProcessLineCount++;
	        	}

	        	statusWriter.writeMessage(String.format("Processed %d lines\n", iProcessLineCount));

	        }

        	printTime();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
    }

	private static void printTime() {		
    	System.out.println( _simpleDateFormat.format(Calendar.getInstance().getTime()) );
	}
	
	private static void dropCollection(String collectionName) {

        MongoClient mongoClient;
        try {
			mongoClient = new MongoClient( "localhost" );

			DB db = mongoClient.getDB( "trufa" );
        
	        DBCollection collection = db.getCollection(collectionName);

	        collection.drop();
        
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

package com.jobsite.trufa.fileReaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class SwanFileReader extends FileReaderBase {

	private File _file = null;
    private BufferedReader _br = null;
    private String _sCurrentLine = null;
	private List<String> _currentLineColumns = new ArrayList<String>();
	private List<String> _headerColumns = new ArrayList<String>();

	private static String SWFR_FULLNAME = "Full name";
	private static String SWFR_CANONICAL_URL = "LinkedIn Profile";
	private static String SWFR_URL = "LinkedIn Profile";

	public SwanFileReader(File file) {
		_file = file;
	
		try {
 
			_br = new BufferedReader(new FileReader(_file));

			String sHeaderLine = readLine(_br);
			_headerColumns = splitLine(sHeaderLine);

		} catch (IOException e) {
			e.printStackTrace();
	    } catch (NullPointerException e) {
			e.printStackTrace();
		}
		
	}

	public boolean hasNext() {

		if (_headerColumns == null || _headerColumns.size() == 0)
			return false;
		
		if(_br == null)
			return false;

		try {
			return ((_sCurrentLine = readLine(_br)) != null);
	    } catch (NullPointerException e) {
			e.printStackTrace();
		}
			
		return false;
	}

	public String next() {
		
		_currentLineColumns.clear();
		
		_currentLineColumns = splitLine(_sCurrentLine);
		if (_currentLineColumns.size() != _headerColumns.size()) {
			System.out.println(
					String.format("Found (%d) columns, expected (%d). Ignoring line %s", _currentLineColumns.size(), _headerColumns.size(), _sCurrentLine));
			return null;
		}

		String json = toJSON(_currentLineColumns);
		
		DBObject dbObject = (DBObject)JSON.parse(json);
		dbObject.put(BASE_TRUFANAME, (String)dbObject.get(SWFR_FULLNAME));
		dbObject.put(BASE_CANONICAL_URL, (String)dbObject.get(SWFR_CANONICAL_URL));
		dbObject.put(BASE_URL, (String)dbObject.get(SWFR_URL));
		return dbObject.toString();
	}
	
	public void close() {
		
		if(_br == null)
			return;
		
		try {
			 
			_br.close();

		} catch (IOException e) {
			e.printStackTrace();
	    } catch (NullPointerException e) {
			e.printStackTrace();
		}
	}
	
	private String toJSON(List<String> currentLineColumns) {
		
		StringBuffer sbJSON = new StringBuffer("{");
		int columnNumber = 0;
		
		int totalNumberOfColumns = currentLineColumns.size();
		for(String column : currentLineColumns) {
			sbJSON.append("\"").append(_headerColumns.get(columnNumber)).append("\":\"").append(column).append("\"");
			columnNumber++;
			
			if (columnNumber < totalNumberOfColumns) {
				sbJSON.append(",");
			}
		}
		
		sbJSON.append("}");
		
		return sbJSON.toString();
	}
	
	private List<String> splitLine(String sLine) {
		
		List<String> cols = new ArrayList<String>(); 		
		boolean inQuotes = false;
		String currentColumn = "";
		for (int i=0;i<sLine.length();++i) {
			char c = sLine.charAt(i);
			
			if(c == '\"') {
				inQuotes = !inQuotes;
			}
			
			if(!inQuotes && c == ',') {
				cols.add(currentColumn);
				currentColumn = "";
			} else {
				if (c != '\"') {
					currentColumn += c;
				}
			}
		}

		cols.add(currentColumn);
/*		// Add any columns that are still missing to make up the same number as required.
		for(int i=cols.size();i<_headerColumns.size();i++) {
			cols.add("");
		}
*/
		return cols;
	}
	
	private String readLine(BufferedReader br) {

		int value = -1;
		char currentChar;
		boolean inQuotes = false;
		StringBuffer sbCurrentLine = new StringBuffer();
		try {
			while((value = br.read()) != -1) {
				currentChar = (char)value; 

				if(currentChar == '\"') {
					inQuotes = !inQuotes;
				}

				if(!inQuotes && (currentChar == '\r' || currentChar == '\n')) {
					br.mark(10);
					while((value = br.read()) != -1) {
						currentChar = (char)value;
						if ((currentChar != '\r' && currentChar != '\n')) {
							br.reset();
							break;
						} else {
							br.mark(10);
						}
					}
					
					return sbCurrentLine.toString();
				} else {
					sbCurrentLine.append(currentChar); 
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (value > 0)
			return sbCurrentLine.toString();
		else 
			return null;
	}
}

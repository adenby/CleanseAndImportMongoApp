package com.jobsite.trufa.fileReaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.jobsite.trufa.util.DateTimeFieldCleanser;
import com.jobsite.trufa.util.DateTimeFieldCleanser.DateTimeFieldTypes;
import com.jobsite.trufa.util.MatchPatternInfo;
import com.jobsite.trufa.util.MatchPatternResultFormat;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class ScrapinghubFileReader extends FileReaderBase {

//	private static String SHFR_FULLNAME = "full_name";
//	private static String SHFR_CANONICAL_URL = "canonical_url";
//	private static String SHFR_URL = "url";

	private File _file = null;
    private BufferedReader _br = null;
	private String _sCurrentLine;

	private Map<String, DateTimeFieldCleanser.DateTimeFieldTypes> _experienceCleanseFields = new HashMap<String, DateTimeFieldCleanser.DateTimeFieldTypes>();
	private Map<String, DateTimeFieldCleanser.DateTimeFieldTypes> _educationCleanseFields = new HashMap<String, DateTimeFieldCleanser.DateTimeFieldTypes>();

	private DateTimeFormatter _dateFormatter = ISODateTimeFormat.dateTimeNoMillis();
	private DecimalFormat _df2 = new DecimalFormat("00");
	private DecimalFormat _df4 = new DecimalFormat("0000");

	public ScrapinghubFileReader(File file) {
		_file = file;

		_dateFormatter = _dateFormatter.withZone(DateTimeZone.forOffsetHours(0));
		
		_educationCleanseFields.put("start", DateTimeFieldTypes.Date);
		_educationCleanseFields.put("end", DateTimeFieldTypes.Date);
		_experienceCleanseFields.put("start", DateTimeFieldTypes.Date);
		_experienceCleanseFields.put("end", DateTimeFieldTypes.Date);
		_experienceCleanseFields.put("duration", DateTimeFieldTypes.Duration);

		try {
 
			_br = new BufferedReader(new FileReader(_file));
 
		} catch (IOException e) {
			e.printStackTrace();
	    } catch (NullPointerException e) {
			e.printStackTrace();
		}
		
	}

	public boolean hasNext() {
		if(_br == null)
			return false;
		
		try {			 
			return ((_sCurrentLine = _br.readLine()) != null);
 
		} catch (IOException e) {
			e.printStackTrace();
	    } catch (NullPointerException e) {
			e.printStackTrace();
		}
			
		return false;
	}
	
	public String next() {
		DBObject dbObject = (DBObject)JSON.parse(_sCurrentLine);
	
		dbObject = cleanse(dbObject);
//		dbObject.put(BASE_TRUFANAME, (String)dbObject.get(SHFR_FULLNAME));
//		dbObject.put(BASE_CANONICAL_URL, (String)dbObject.get(SHFR_CANONICAL_URL));
//		dbObject.put(BASE_URL, (String)dbObject.get(SHFR_URL));
		
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
	
	private DBObject cleanse(DBObject dbObject) {
		dbObject = cleanseDateField("updated", dbObject);
		
		dbObject = cleanseSimpleListValue("experience",_experienceCleanseFields,dbObject);
		dbObject = cleanseSimpleListValue("education",_educationCleanseFields,dbObject);

		return dbObject;
	}
	
	private DBObject cleanseDateField(String fieldname, DBObject dbObject) {

		if (dbObject.containsField(fieldname)) {
			String dateField = (String)dbObject.get(fieldname);
			dateField += "+00:00";

			DateTime dt = _dateFormatter.parseDateTime(dateField);
			
			String mon = _df2.format(dt.getMonthOfYear());
			
			String strNewValueStr = String.format("%s%s%s%s%s%s", 
					_df4.format(dt.getYear()), 
					_df2.format(dt.getMonthOfYear()), 
					_df2.format(dt.getDayOfMonth()), 
					_df2.format(dt.getHourOfDay()), 
					_df2.format(dt.getMinuteOfHour()), 
					_df2.format(dt.getSecondOfMinute()));

			strNewValueStr = strNewValueStr.replaceAll("0*$", "");
			Long lNewValue = Long.parseLong(strNewValueStr);
			
//			java.util.Date d = new java.util.Date();

			dbObject.put(fieldname, lNewValue);
//			dbObject.put(fieldname, d);
		}
		
		return dbObject;
	}
	
	private DBObject cleanseSimpleListValue(String listName, Map<String, DateTimeFieldCleanser.DateTimeFieldTypes> fieldsMap, DBObject dbObject) {

		if (!dbObject.containsField(listName)) {
			return dbObject;
		}

		BasicDBList simpleList = (BasicDBList)dbObject.get(listName);
		
		Iterator<Object> simpleListIter = simpleList.iterator();
		if(simpleListIter.hasNext()) {
			DBObject simpleListItem = (DBObject)simpleListIter.next();

			Iterator<String> fieldsSetIter = fieldsMap.keySet().iterator();
			
			while(fieldsSetIter.hasNext()) {
				String fieldName = fieldsSetIter.next();
				DateTimeFieldCleanser.DateTimeFieldTypes dateTimeFieldTypes = fieldsMap.get(fieldName);
				
				if (simpleListItem.containsField(fieldName)) {
					String fieldNameValue = (String)simpleListItem.get(fieldName);
		
					simpleListItem.put(fieldName, cleansStartEndDuration(fieldNameValue, dateTimeFieldTypes));
				}	
			}
		}

		return dbObject;
	}

	private String cleansStartEndDuration(String fieldValue, DateTimeFieldCleanser.DateTimeFieldTypes dateTimeFieldTypes) {

		DateTimeFieldCleanser dateTimeFieldCleanser = new DateTimeFieldCleanser();
		if ("present".equals(fieldValue.toLowerCase()) || "aktuell".equals(fieldValue.toLowerCase())) {
//			System.out.println("0. MatchResult:  Input: " + fieldValue + ", Output: 000000");
			fieldValue = "000000";
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "\\((\\d+)\\syears\\s(\\d+)\\smonths\\)", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2}, 
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}), 
			dateTimeFieldTypes))) { // e.g. (10 years 5 months) 
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("1. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "\\((\\d+)\\syear\\s(\\d+)\\smonths\\)", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2},
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),
			dateTimeFieldTypes))) {  // e.g. (1 year 5 months)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("2. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "\\((\\d+)\\syears\\s(\\d+)\\smonth\\)", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2},
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) {  // e.g. (10 years 1 month)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("3. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "\\((\\d+)\\syear\\s(\\d+)\\smonth\\)", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2}, 
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) {   // e.g. (1 year 1 month)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("4. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "(\\d+)\\syears\\s(\\d+)\\smonths",
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2},
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) { // e.g. 10 years 5 months
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("5. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "(\\d+)\\syear\\s(\\d+)\\smonths", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2}, 
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) {  // e.g. 1 year 5 months
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("6. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "(\\d+)\\syears\\s(\\d+)\\smonth", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2}, 
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) {  // e.g. 10 years 1 month
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("7. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
			new MatchPatternInfo(fieldValue, "(\\d+)\\syear\\s(\\d+)\\smonth", 
			new MatchPatternResultFormat( 	new DecimalFormat[] {_df4, _df2}, 
											new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR, MatchPatternResultFormat.DateComponentType.MONTH}),						
			dateTimeFieldTypes))) {   // e.g. 1 year 1 month
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("8. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "\\((\\d+)\\smonths\\)", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df2},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.MONTH}),
				dateTimeFieldTypes))) { // e.g. (7 months)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\d+)\\smonths", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df2},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.MONTH}),
				dateTimeFieldTypes))) { // e.g. 7 months
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "\\((\\d+)\\smonth\\)", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df2},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.MONTH}),
				dateTimeFieldTypes))) { // e.g. (1 month)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\d+)\\smonth", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df2},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.MONTH}),
				dateTimeFieldTypes))) { // e.g. 1 month
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "\\((\\d+)\\syears\\)", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df4},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. (7 years)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\d+)\\syears", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df4},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. 7 years
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "\\((\\d+)\\syear\\)", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df4},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. (1 year)
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());
		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\d+)\\syear", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df4},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. 1 year
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("9. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

			
		} /*else if (dateTimeFieldCleanser.getMatch(new MatchPatternInfo(fieldValue, "(\\w+)\\s(\\d{4})", new DecimalFormat[] {null, _df4}, dateTimeFieldTypes))) { // e.g. June 2010 
			System.out.println("8. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} */ else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\w+)\\s(\\d+)", 
				new MatchPatternResultFormat(	new DecimalFormat[] {null, _df4}, 
						new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.MONTH, MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. June 2014 
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("10. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} else if (dateTimeFieldCleanser.getMatch(
				new MatchPatternInfo(fieldValue, "(\\d{4})", 
				new MatchPatternResultFormat(	new DecimalFormat[] {_df4},
												new MatchPatternResultFormat.DateComponentType[] {MatchPatternResultFormat.DateComponentType.YEAR}),
				dateTimeFieldTypes))) { // e.g. 2010
			fieldValue = dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue(); 
//			System.out.println("11. MatchResult:  Input: " + dateTimeFieldCleanser.getMatchPatternInfo().getInputValue() + ", Output: " + dateTimeFieldCleanser.getMatchPatternInfo().getOutputValue());

		} else {
			System.out.println("UNKNOWN RESULT: " + fieldValue + ", Unknown");
		}
		
		return fieldValue;
	}
}

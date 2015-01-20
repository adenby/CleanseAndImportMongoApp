package com.jobsite.trufa.util;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeFieldCleanser {

	public enum DateTimeFieldTypes {
		ISO8601DateTime,
		Date,
		Duration
	}
	
	private MatchPatternInfo _matchPatternInfo = null;
	
	public boolean getMatch(MatchPatternInfo matchPatternInfo) {

		_matchPatternInfo = matchPatternInfo;
matchPatternInfo.setOutputValue("PATTERN FAILED");
		
	    // Create a Pattern object
	    Pattern p = Pattern.compile(matchPatternInfo.getPattern(), Pattern.CASE_INSENSITIVE);

	    // Now create matcher object.
	    Matcher matcher = p.matcher(matchPatternInfo.getInputValue());
	    //Matcher matcher = p.matcher(fieldValue);

	    if (matcher.find() && matcher.matches()) {
	    	String strNewValueStr = "";
    		MatchPatternResultFormat matchPatternResultFormat = matchPatternInfo.getMatchPatternResultFormat();
    		String month = "00";
    		String year = "0000";
	    	for(int i=0;i<matcher.groupCount();++i) {
	    		DecimalFormat decimalFormat = matchPatternResultFormat.getDecimalFormat(i);
	    		MatchPatternResultFormat.DateComponentType dateComponentType = matchPatternResultFormat.getDateComponentType(i);
	    		if (decimalFormat != null) {
	    			Integer value = Integer.parseInt(matcher.group(i+1));

	    			if (MatchPatternResultFormat.DateComponentType.YEAR.equals(dateComponentType)) {
	    				year = String.format("%s",decimalFormat.format(value));
	    			} else if (MatchPatternResultFormat.DateComponentType.MONTH.equals(dateComponentType)) {
	    				month = String.format("%s",decimalFormat.format(value));
	    			}
	    		} else {
	    			String value = matcher.group(i+1);
	    			if ("january".equals(value.toLowerCase())) { 
	    				value = "01";
	    			} else if ("february".equals(value.toLowerCase())) {
	    				value = "02";
	    			} else if ("march".equals(value.toLowerCase())) {
	    				value = "03";
	    			} else if ("april".equals(value.toLowerCase())) {
	    				value = "04";
	    			} else if ("may".equals(value.toLowerCase())) {
	    				value = "05";
	    			} else if ("june".equals(value.toLowerCase())) {
	    				value = "06";
	    			} else if ("july".equals(value.toLowerCase())) {
	    				value = "07";
	    			} else if ("august".equals(value.toLowerCase())) {
	    				value = "08";
	    			} else if ("september".equals(value.toLowerCase())) {
	    				value = "09";
	    			} else if ("october".equals(value.toLowerCase())) {
	    				value = "10";
	    			} else if ("november".equals(value.toLowerCase())) {
	    				value = "11";
	    			} else if ("december".equals(value.toLowerCase())) {
	    				value = "12";
	    			}

	    			if (MatchPatternResultFormat.DateComponentType.YEAR.equals(dateComponentType)) {
	    				year = value;
	    			} else if (MatchPatternResultFormat.DateComponentType.MONTH.equals(dateComponentType)) {
	    				month = value;
	    			}
	    		}
	    	}
			strNewValueStr = year + month;  
	    	
//			strNewValueStr = strNewValueStr.replaceAll("0*$", "");
			matchPatternInfo.setOutputValue(strNewValueStr);
//			matchPatternInfo.setOutputValue(Integer.parseInt(strNewValueStr));

//System.out.println("[YEARS]: " + matcher.group(1));
//System.out.println("[MONTHS]: " + matcher.group(2));
	        
	        return true;	        
	    }

		return false;		
	}
	
	public MatchPatternInfo getMatchPatternInfo() {
		return _matchPatternInfo;
	}
}

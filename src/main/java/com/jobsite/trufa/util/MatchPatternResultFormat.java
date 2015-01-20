package com.jobsite.trufa.util;

import java.text.DecimalFormat;

public class MatchPatternResultFormat {

	public enum DateComponentType {
		YEAR,
		MONTH
	}

	private DecimalFormat[] _decimalFormat = null;
	private DateComponentType[] _dateComponentType = null;	

	public MatchPatternResultFormat(DecimalFormat[] decimalFormat, DateComponentType[] dateComponentType) { 
		_decimalFormat = decimalFormat;
		_dateComponentType = dateComponentType; 
	}

	public DecimalFormat getDecimalFormat(int index) {

		if (index > _decimalFormat.length) {
			System.out.println("Invalid DecimalFormat list specified");
			return null;
		}		
		return _decimalFormat[index];
	}

	public DateComponentType getDateComponentType(int index) {

		if (index > _dateComponentType.length) {
			System.out.println("Invalid DateComponentType list specified");
			return null;
		}		
		return _dateComponentType[index];
	}
}

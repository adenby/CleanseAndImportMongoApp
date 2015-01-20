package com.jobsite.trufa.util;

public class MatchPatternInfo {

	private String _pattern;
	private MatchPatternResultFormat _matchPatternResultFormat = null;
	private String _inputValue;
	private String _outputValue;
	private DateTimeFieldCleanser.DateTimeFieldTypes _dateTimeFieldTypes; 

	public MatchPatternInfo(String inputValue, String pattern, MatchPatternResultFormat matchPatternResultFormat, DateTimeFieldCleanser.DateTimeFieldTypes dateTimeFieldTypes) {
		_pattern = pattern;
		_matchPatternResultFormat = matchPatternResultFormat;
		_inputValue = inputValue;
		_dateTimeFieldTypes = dateTimeFieldTypes;
	}
	
	public String getPattern() {
		return _pattern;
	}
	
	public String getInputValue() {
		return _inputValue;
	}

	public String getOutputValue() {
		return _outputValue;
	}
	
	public void setOutputValue(String outputValue) {
		_outputValue = outputValue;
	}
	
	public DateTimeFieldCleanser.DateTimeFieldTypes getDateTimeFieldTypes() {
		return _dateTimeFieldTypes;
	}
	
	public MatchPatternResultFormat getMatchPatternResultFormat() {			
		return _matchPatternResultFormat;
	}

	@Override
	public String toString() {
		return String.format("Pattern: [%s], InputValue: [%s], OuputValue: [%s]", _pattern, _inputValue, _outputValue);
	}
	

}

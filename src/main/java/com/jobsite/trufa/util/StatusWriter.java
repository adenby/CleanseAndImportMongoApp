package com.jobsite.trufa.util;

public class StatusWriter {

	private static StatusWriter _instance = null;
	
	protected StatusWriter() {
		// Exists only to defeat instantiation.
	}
   
	public static StatusWriter getInstance() {
	      if(_instance == null) {
	         _instance = new StatusWriter();
	      }
	      return _instance;
	}
	
	private int _previousMessageLength = -1;
	
	public void writeMessage(String message) {
		while (_previousMessageLength > 0) {
			System.out.print("\b");
			--_previousMessageLength;
		}
		
		System.out.print(message);
		_previousMessageLength = message.length();
	}
}

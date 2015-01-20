package com.jobsite.trufa.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {
	
	private File _outFile = null;
	private BufferedWriter _bw = null;
	
	public Logger(File outFile) {
		_outFile = outFile;
		
		try {
			// if file doesnt exists, then create it
			if (!_outFile.exists()) {
				_outFile.createNewFile();
			}

			FileWriter fw = new FileWriter(_outFile.getAbsoluteFile());
			_bw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public void print(String value) {
		
		if (_bw == null) {
			return;
		}
		
		try {
 
			_bw.write(value);
  
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public void close() {
		if (_bw != null) {
			try {
				_bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}

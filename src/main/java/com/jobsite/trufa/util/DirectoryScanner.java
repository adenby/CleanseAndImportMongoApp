package com.jobsite.trufa.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

public class DirectoryScanner {

	private File _dataDirectory = null;
	private List<File> _dataFiles = new ArrayList<File>();
	private int _currPos = 0;
	
	public DirectoryScanner(File dataDirectory, FilenameFilter filenameFilter) {

		_dataDirectory = dataDirectory;

		File [] files = _dataDirectory.listFiles(filenameFilter);

		for (File jlfile : files) {
			_dataFiles.add(jlfile);
		}
	}
	
	public boolean hasNext() {
		
		if (_currPos < _dataFiles.size())
			return true;

		return false;
	}
	
	public File next() {
		File file = _dataFiles.get(_currPos);
		_currPos++;
		return file;
	}
	
	public void reset() {
		_currPos = 0;
	}
}

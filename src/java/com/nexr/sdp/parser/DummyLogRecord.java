package com.nexr.sdp.parser;

import java.util.HashMap;
import java.util.Map;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;

/**
 * Test
 * @author Daegeun Kim
 */
public class DummyLogRecord extends LogRecord {
	Map<String, Object> values = new HashMap<String, Object>();
	
	public void add(String name, String value) {
		values.put(name, value);
	}
	
	public Map<String, Object> values() {
		return values;
	}
}

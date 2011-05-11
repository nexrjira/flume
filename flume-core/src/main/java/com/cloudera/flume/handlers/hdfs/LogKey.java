package com.cloudera.flume.handlers.hdfs;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.core.Event;

public class LogKey extends DetailEventKey {
	
	public static final String DATA_TYPE = "dataType";
	public static final String TIME = "time";
	public static final String LOG_ID= "logId";

	public LogKey(Event e) {
		super(e);
		List<String> keys = new ArrayList<String>();
		keys.add(DATA_TYPE);
		keys.add(TIME);
		keys.add(LOG_ID);
		init(keys);
	}
}

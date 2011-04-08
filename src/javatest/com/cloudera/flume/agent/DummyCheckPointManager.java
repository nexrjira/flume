package com.cloudera.flume.agent;

import java.util.Map;

public class DummyCheckPointManager implements ICheckPointManager {
	
	private Map<String, Long> checkPointMap;

	@Override
	public Map<String, Long> getCheckPoint(String logicalNodeName) {
		return checkPointMap;
	}
	
	public void setCheckPointMap(Map<String, Long> checkPointMap) {
		this.checkPointMap = checkPointMap;
	}

}

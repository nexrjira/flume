package com.cloudera.flume.agent;

import java.util.Map;

public interface ICheckPointManager {
	public Map<String, Long> getCheckPoint(String logicalNodeName);
}

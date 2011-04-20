package com.cloudera.flume.agent;

import java.util.List;
import java.util.Map;

import com.nexr.agent.cp.CheckPointManager;

public class DummyCheckPointManager implements CheckPointManager {
	
	private Map<String, Long> checkPointMap;
	
	@Override
	public String getTagId(String agentName, String filename) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startClient(String collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopClient() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addPendingQ(String tagId, Map<String, Long> tagContent) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Long> getOffset(String logicalNodeName) {
		return checkPointMap;
	}

	@Override
	public void setCollectorHost(String host) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startTagChecker() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopTagChecker() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startServer() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addCollectorPendingList(String tagId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void moveToCompleteList() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> getTagList(String agentName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setCheckPointMap(Map<String, Long> checkPointMap) {
		this.checkPointMap = checkPointMap;
	}
}

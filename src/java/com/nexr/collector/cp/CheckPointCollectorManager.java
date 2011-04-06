package com.nexr.collector.cp;

import java.util.List;

public interface CheckPointCollectorManager {
	
	public void addPandingList(String tagId);
	
//	public void updatePandingList(String tagId);
	
	public void moveToCompleteList();
	
	public List<String> getTagList(String agentName);
}

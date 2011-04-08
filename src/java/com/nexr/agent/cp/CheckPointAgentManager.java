package com.nexr.agent.cp;


public interface CheckPointAgentManager {

	public String getTagId(String agentName, String filename);
	
	public void addPandingQ(String filename, String tagId, long offset);
	
//	public void updateCheckPointFile(String filename, String tagId);
	
	public String getOffset(String filename);
	
	public void setCollectorHost(String host);
}

package com.nexr.agent.cp;


public interface CheckPointAgentManager {

	public String getTagId(String filename);
	
	public void addPandingQ(String filename, String tagId, long offset);
	
	public void updateCheckPointFile(String filename, String tagId);
}

package com.nexr.agent.cp;

import java.util.Map;

import org.mortbay.log.Log;

public class WaitingQueueModel {
	
	String tagId;
	Map<String, Long> contents; //<FileName, Offset>
	long waitedTime;
	
	public WaitingQueueModel(String tagId, Map<String, Long> contents, long waitedTime) {
		
		this.tagId = tagId;
		this.contents = contents;
		this.waitedTime = waitedTime;
	}
	
	public String getTagId() {
		return tagId;
	}
	public void setTagId(String tagId) {
		this.tagId = tagId;
	}
	public Map<String, Long> getContents() {
		return contents;
	}
	public void setContents(Map<String, Long> contents) {
		this.contents = contents;
	}

	public long getWaitedTime() {
		return waitedTime;
	}

	public void setWaitedTime(long waitedTime) {
		this.waitedTime = waitedTime;
	}
	
	public void updateWaitedTime(long waitedTime) {
		this.waitedTime = this.waitedTime + waitedTime;
		System.out.println(this.waitedTime);
	}

	
	
}

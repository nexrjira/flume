package com.nexr.agent.cp;

import java.util.Map;

public class PendingQueueModel {
	
	String tagId;
	Map<String, Long> contents; //<FileName, Offset>
	
	public PendingQueueModel(String tagId, Map<String, Long> contents) {
		this.tagId = tagId;
		this.contents = contents;
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
	
	
}

package com.nexr.agent.cp;


public class PendingEvent implements Comparable<PendingEvent> {
	private String tagId;
	private long offset;
	
	public PendingEvent(String tagId, long offset){
		this.tagId = tagId;
		this.offset = offset;
	}
	
	public String getTagId() {
		return tagId;
	}
	public void setTagId(String tagId) {
		this.tagId = tagId;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public int compareTo(PendingEvent o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

}

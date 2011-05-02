package com.nexr.agent.cp;

public class CollectorInfo {
	String collectorHost;
	int collectorPort;
	
	public CollectorInfo(String collectorHost,
			int collectorPort) {
		this.collectorHost = collectorHost;
		this.collectorPort = collectorPort;
	}
	
	public String getCollectorHost() {
		return collectorHost;
	}
	public void setCollectorHost(String collectorHost) {
		this.collectorHost = collectorHost;
	}
	public int getCollectorPort() {
		return collectorPort;
	}
	public void setCollectorPort(int collectorPort) {
		this.collectorPort = collectorPort;
	}
	
	
}

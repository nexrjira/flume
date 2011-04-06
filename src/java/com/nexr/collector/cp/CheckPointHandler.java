package com.nexr.collector.cp;

import java.util.List;

import org.apache.thrift.TException;

import com.nexr.cp.thrift.*;

public class CheckPointHandler implements CheckPointService.Iface{
	
	CheckPointCollectorManager manager = CheckPointCollectorManagerImpl.getInstance();
	
	@Override
	public List<String> checkTagId(String agentName)
			throws TException {
		// TODO Auto-generated method stub
		List<String> tags = manager.getTagList(agentName);
		return tags;
	}
}

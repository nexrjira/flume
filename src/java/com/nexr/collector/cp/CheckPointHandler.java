package com.nexr.collector.cp;

import java.util.List;

import org.apache.thrift.TException;

import com.cloudera.flume.agent.FlumeNode;
import com.nexr.agent.cp.CheckPointManager;
import com.nexr.agent.cp.CheckPointManagerImpl;
import com.nexr.cp.thrift.CheckPointService;

public class CheckPointHandler implements CheckPointService.Iface{
	
	
	CheckPointManager manager = null;
	
	public CheckPointHandler() {
		manager = FlumeNode.getInstance().getCheckPointManager();
	}
	
	@Override
	public List<String> checkTagId(String agentName)
			throws TException {
		// TODO Auto-generated method stub
		List<String> tags = manager.getTagList(agentName);
		return tags;
	}
}

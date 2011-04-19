package com.nexr.collector.cp;

import org.apache.thrift.TException;

import com.nexr.agent.cp.CheckPointManager;
import com.nexr.agent.cp.CheckPointManagerImpl;
import com.nexr.cp.thrift.CheckPointService;

public class CheckPointHandler implements CheckPointService.Iface{
	
	CheckPointManager manager = CheckPointManagerImpl.getInstance();
	
	@Override
	public boolean checkTagId(String tagId)
			throws TException {
		// TODO Auto-generated method stub
		return manager.getTagList(tagId);
	}
}

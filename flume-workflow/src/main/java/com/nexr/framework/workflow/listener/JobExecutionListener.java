package com.nexr.framework.workflow.listener;

import com.nexr.framework.workflow.Job;

/**
 * @author dani.kim@nexr.com
 */
public interface JobExecutionListener {
	void beforeJob(Job job);
	
	void afterJob(Job job);
}

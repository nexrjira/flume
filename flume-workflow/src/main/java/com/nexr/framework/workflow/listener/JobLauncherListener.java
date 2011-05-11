package com.nexr.framework.workflow.listener;

import com.nexr.framework.workflow.JobExecution;

/**
 * @author dani.kim@nexr.com
 */
public interface JobLauncherListener {
	void failure(JobExecution execution);
}

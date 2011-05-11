package com.nexr.framework.workflow;

public interface JobLauncher {
	JobExecution run(Job job);
}

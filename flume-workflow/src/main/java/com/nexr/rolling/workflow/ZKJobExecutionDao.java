package com.nexr.rolling.workflow;

import org.springframework.stereotype.Repository;

import com.nexr.framework.workflow.Job;
import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobExecutionDao;
import com.nexr.framework.workflow.Workflow;

/**
 * @author dani.kim@nexr.com
 */
@Repository
public class ZKJobExecutionDao implements JobExecutionDao {
	public ZKJobExecutionDao() {
	}

	@Override
	public JobExecution saveJobExecution(Job job) {
		JobExecution execution = new JobExecution();
		execution.setWorkflow(new Workflow(job));
		return execution;
	}
	
	@Override
	public JobExecution updateJobExecution(JobExecution execution) {
		return execution;
	}

	@Override
	public JobExecution getJobExecution(Job job) {
		return null;
	}
}

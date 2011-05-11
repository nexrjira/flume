package com.nexr.framework.workflow;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.nexr.framework.workflow.listener.JobLauncherListener;

/**
 * @author dani.kim@nexr.com
 */
@Component
public class JobLauncherImpl implements JobLauncher {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Resource
	JobExecutionDao executionDao;
	JobLauncherListener listener;
	
	@Override
	public JobExecution run(Job job) {
		Assert.notNull(job);
		Assert.notNull(job.getSteps());
		
		LOG.info("start job {}", job);
		JobExecution execution = executionDao.getJobExecution(job);
		if (execution == null) {
			execution = executionDao.saveJobExecution(job);
		}
		try {
			job.execute(execution);
		} catch (Exception e) {
			e.printStackTrace();
			execution.setStatus(JobStatus.FAILED);
			executionDao.updateJobExecution(execution);
			if (listener != null) {
				listener.failure(execution);
			}
			
		}
		LOG.info("end job {}, {}", job, execution.getWorkflow());
		return execution;
	}
}

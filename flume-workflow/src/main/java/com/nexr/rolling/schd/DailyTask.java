package com.nexr.rolling.schd;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;	

import com.nexr.rolling.core.RollingConfig;

public class DailyTask extends QuartzJobBean {
	private static final Logger log = Logger.getLogger(DailyTask.class);
	
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		// TODO Auto-generated method stub
		RollingConfig config = (RollingConfig) context.getJobDetail()
				.getJobDataMap().get("config");

		log.info("DailyRolling Job Start");
	}

}

package com.nexr.rolling.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.framework.workflow.JobExecution;
import com.nexr.framework.workflow.JobLauncher;
import com.nexr.framework.workflow.SimpleJob;
import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;
import com.nexr.framework.workflow.Steps;
import com.nexr.framework.workflow.listener.StepExecutionListener;
import com.nexr.rolling.workflow.hourly.CleanUpTasklet;
import com.nexr.rolling.workflow.hourly.FinishingTasklet;
import com.nexr.rolling.workflow.hourly.InitTasklet;
import com.nexr.rolling.workflow.hourly.PrepareTasklet;

/**
 * @author dani.kim@nexr.com
 */
public class HourlyRollingJob extends SimpleJob {
	private static final Logger LOG = LoggerFactory.getLogger(HourlyRollingJob.class);
	private final static Steps steps = new Steps();
	
	static {
		steps.add(new Step("init", InitTasklet.class));
		steps.add(new Step("prepare", PrepareTasklet.class));
//		steps.add(new Step("run", RunHourlyTasklet.class));
		steps.add(new Step("finishing", FinishingTasklet.class));
		steps.add(new Step("cleanUp", CleanUpTasklet.class));
	}

	public HourlyRollingJob() {
		super("hourlyRollingJob", steps, true);
	}
	
	public static void main(String[] args) throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:workflow-app.xml");
		JobLauncher launcher = context.getBean(JobLauncher.class);

		HourlyRollingJob job = new HourlyRollingJob();
		job.setListener(new StepExecutionListener() {
			@Override
			public void beforeStep(Step step, StepContext context) {
				// ZK 상태 변경
			}
			
			@Override
			public void afterStep(Step step, StepContext context) {
				// ZK 상태 변경
			}
			
			@Override
			public void caught(Step step, StepContext context, Throwable cause) {
				System.out.println(cause);
				System.out.println(cause);
				System.out.println(cause);
				System.out.println(cause);
				System.out.println(cause);
				System.out.println(cause);
			}
		});
		job.setExecutionDao(new ZKJobExecutionDao());
		job.addParameter(RollingConstants.HOURLY_MR_RAW_PATH, "/nexr/rolling/hourly/raw");
		job.addParameter(RollingConstants.HOURLY_MR_INPUT_PATH, "/nexr/rolling/hourly/input");
		job.addParameter(RollingConstants.HOURLY_MR_OUTPUT_PATH, "/nexr/rolling/hourly/output");
		job.addParameter(RollingConstants.HOURLY_MR_RESULT_PATH, "/nexr/rolling/hourly/result");
		
		JobExecution execution = launcher.run(job);
		LOG.info("Workflow : {}", execution.getWorkflow());
		
	}
}

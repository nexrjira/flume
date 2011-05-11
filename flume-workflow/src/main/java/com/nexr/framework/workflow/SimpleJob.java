package com.nexr.framework.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dani.kim@nexr.com
 */
public class SimpleJob extends Job {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public SimpleJob() {
		super();
	}
	
	public SimpleJob(String name, Steps steps) {
		super(name, steps);
	}
	
	public SimpleJob(String name, Steps steps, boolean recoverable) {
		super(name, steps, recoverable);
	}
	
	@Override
	protected void doExecute(JobExecution execution) throws Exception {
		StepContext context = new StepContext();
		context.setConfig(new StepContext.Config(getParameters()));

		Workflow workflow = execution.getWorkflow();
		for (Step step : getSteps()) {
			workflow.addStep(step);
			try {
				steplistener.beforeStep(step, context);
			} catch (Exception e) {
				LOG.warn("exception encountered in before callback of step", e);
			}
			Tasklet tasklet = step.getTasklet().newInstance();
			try {
				tasklet.run(context);
				steplistener.afterStep(step, context);
			} catch (Exception e) {
				LOG.warn("exception encountered in after callback of step", e);
				try {
					steplistener.caught(step, context, e);
				} catch (Exception e2) {
					LOG.warn("exception encountered in caught callback of step", e);
				}
			}
		}
//		if (workflow == null) {
//			workflow = new Workflow(this);
//			execution.setWorkflow(workflow);
//		}
//		Step currentStep = workflow.forward(null);
//		while (true) {
//			LOG.info("start step {}, {}", this, currentStep);
//			String next = stepHandler.execute(currentStep, context);
//			LOG.info("end step {}, {}", this, currentStep);
//			currentStep = workflow.forward(next);
//			if (currentStep == null) {
//				break;
//			}
//		}
	}
}

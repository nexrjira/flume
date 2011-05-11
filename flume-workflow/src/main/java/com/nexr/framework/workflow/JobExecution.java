package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class JobExecution {
	private JobStatus status;
	private StepContext context;
	private Workflow workflow;
	
	public JobExecution() {
	}

	public synchronized JobStatus getStatus() {
		return status;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	public void setWorkflow(Workflow workflow) {
		this.workflow = workflow;
	}

	public synchronized void setStatus(JobStatus status) {
		this.status = status;
		notifyAll();
	}

	public StepContext getContext() {
		return context;
	}
	
	public void setContext(StepContext context) {
		this.context = context;
	}
}

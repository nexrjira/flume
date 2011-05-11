package com.nexr.framework.workflow.listener;

import com.nexr.framework.workflow.Step;
import com.nexr.framework.workflow.StepContext;

/**
 * @author dani.kim@nexr.com
 */
public interface StepExecutionListener {
	void beforeStep(Step step, StepContext context);
	
	void afterStep(Step step, StepContext context);
	
	void caught(Step step, StepContext context, Throwable cause);
}

package com.nexr.framework.workflow;

/**
 * Step 별 실행할 인터페이스
 * @author dani.kim@nexr.com
 */
public interface Tasklet {
	String run(StepContext context);
}

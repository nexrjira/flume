package com.nexr.framework.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public class Steps implements Iterable<Step> {
	private List<Step> steps;
	private Map<String, Step> stepMap;
	
	public Steps() {
		this(new ArrayList<Step>());
	}
	
	public Steps(List<Step> steps) {
		this.steps = new ArrayList<Step>();
		stepMap = new HashMap<String, Step>();
		if (steps != null) {
			for (Step step : steps) {
				this.steps.add(step);
				if (step.getName() == null) {
					step.setName(step.getTasklet().getName());
				}
				stepMap.put(step.getName(), step);
			}
		}
	}

	public Steps(Step[] steps) {
		this(Arrays.asList(steps));
	}

	public Steps(Steps steps) {
		this(steps.steps);
	}
	
	@Override
	public String toString() {
		return steps.toString();
	}
	
	public Steps add(Step step) {
		this.steps.add(step);
		return this;
	}
	
	public Step first() {
		if (steps.size() > 0) {
			return steps.get(0);
		}
		return null;
	}
	
	public Step last() {
		if (steps.size() > 0) {
			return steps.get(steps.size() - 1);
		}
		return null;
	}

	public Step get(String name) {
		return stepMap.get(name);
	}

	@Override
	public Iterator<Step> iterator() {
		return steps.iterator();
	}
}

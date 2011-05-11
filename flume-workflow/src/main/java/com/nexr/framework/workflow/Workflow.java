package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class Workflow {
	private Steps steps;
	private Steps footprints;
	private String current;

	public Workflow(Job job) {
		steps = new Steps(job.getSteps());
		footprints = new Steps();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("workflow[");
		synchronized (footprints) {
			boolean first = true;
			for (Step step : footprints) {
				sb.append(first ? "" : " > ");
				sb.append(step.getName());
				first = false;
			}
		}
		sb.append("]");
		
		return sb.toString();
	}
	
	public void addStep(Step step) {
		footprints.add(step);
	}
	
//	public void setCurrent(String current) {
//		this.current = current;
//	}
//	
//	public Step next() {
//		return forward(current);
//	}
//
//	public Step forward(String name) {
//		if (name == null) {
//			current = steps.first().getName();
//		} else {
//			current = steps.get(name).getNext();
//		}
//		if (current != null) {
//			footprints.add(steps.get(current));
//			return footprints.last();
//		}
//		return null;
//	}
//
//	public boolean hasNext() {
//		Step current = steps.get(this.current);
//		if (current == null || current.getNext() == null) {
//			return false;
//		}
//		return true;
//	}
}

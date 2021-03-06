package com.nexr.framework.workflow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public class StepContext {
	private JobExecution jobExecution;
	private Config config;
	private Map<String, String> context;
	
	public StepContext() {
		context = new HashMap<String, String>();
	}
	
	public JobExecution getJobExecution() {
		return jobExecution;
	}
	
	public void setJobExecution(JobExecution jobExecution) {
		this.jobExecution = jobExecution;
	}
	
	public Collection<String> keys() {
		return context.keySet();
	}
	
	public String get(String name, String defaultValue) {
		return context.get(name) == null ? defaultValue : context.get(name);
	}
	
	public int getInt(String name, int defaultValue) {
		try {
			return Integer.parseInt(get(name, null));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public void set(String name, String value) {
		context.put(name, value);
	}
	
	public Config getConfig() {
		return config;
	}
	
	public void setConfig(Config config) {
		this.config = config;
	}
	
	public static class Config {
		private Map<String, String> parameters;

		public Config(Map<String, String> parameters) {
			this.parameters = parameters;
		}
		
		public String get(String name, String defaultValue) {
			return parameters.get(name) == null ? defaultValue : parameters.get(name);
		}
		
		public Collection<String> keys() {
			return parameters.keySet();
		}
	}
}

package com.nexr.framework.workflow;

import java.util.Map;

/**
 * @author dani.kim@nexr.com
 */
public class StepContext {
	private Config config;
	
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
	}
}

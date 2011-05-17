package com.nexr.rolling.workflow;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.rolling.workflow.DFSTasklet;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
public class InitTasklet extends DFSTasklet {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	public InitTasklet() {
		super();
	}
	
	public String run(com.nexr.framework.workflow.StepContext context) {
		Path input = new Path(context.getConfig().get(RollingConstants.INPUT_PATH, null));
		Path output = new Path(context.getConfig().get(RollingConstants.OUTPUT_PATH, null));
		try {
			if (!fs.exists(input)) {
				fs.mkdirs(input);
			}
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		LOG.info("Rolling Job Success Initialization : Input: " + input.getName());
		return "prepare";
	}
}
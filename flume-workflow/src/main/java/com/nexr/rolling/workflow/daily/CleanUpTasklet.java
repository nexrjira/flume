package com.nexr.rolling.workflow.daily;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
@Deprecated
public class CleanUpTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	@Override
	public String run(StepContext context) {
		String input = context.getConfig().get(RollingConstants.DAILY_MR_INPUT_PATH, null);
		String output = context.getConfig().get(RollingConstants.DAILY_MR_OUTPUT_PATH, null);
		LOG.info("Cleanup : Input: {}, Output: {}", new Object[] { input, output });
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(input), true);
			fs.delete(new Path(output), true);
			fs.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}

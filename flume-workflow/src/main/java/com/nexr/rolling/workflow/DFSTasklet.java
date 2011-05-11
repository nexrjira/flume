package com.nexr.rolling.workflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.nexr.framework.workflow.Tasklet;

/**
 * @author dani.kim@nexr.com
 */
public abstract class DFSTasklet implements Tasklet {
	protected Configuration conf;
	protected FileSystem fs;
	
	public DFSTasklet() {
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

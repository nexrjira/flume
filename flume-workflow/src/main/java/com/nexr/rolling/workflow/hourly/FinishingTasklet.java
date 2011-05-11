package com.nexr.rolling.workflow.hourly;

import java.io.File;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.DFSTasklet;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * @author dani.kim@nexr.com
 */
public class FinishingTasklet extends DFSTasklet {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	final public static PathFilter SEQ_FILE_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().startsWith("part");
		}
	};
	
	@SuppressWarnings("unused")
	@Override
	public String run(StepContext context) {
		String output = context.getConfig().get(RollingConstants.HOURLY_MR_OUTPUT_PATH, null);
		String result = context.getConfig().get(RollingConstants.HOURLY_MR_RESULT_PATH, null);
		
		Path sourcePath = new Path(output);
		FileStatus[] dataType = null;
		FileStatus[] hours = null;
		FileStatus[] files = null;

		try {
			if (!fs.exists(new Path(result))) {
				fs.mkdirs(new Path(result));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		try {
			dataType = fs.listStatus(sourcePath);
			boolean rename;
			String dirName;
			for (FileStatus type : dataType) {
				hours = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus hour : hours) {
					files = fs.listStatus(new Path(sourcePath, type.getPath()
							.getName()+File.separator+hour.getPath()
							.getName()), SEQ_FILE_FILTER);
					for (FileStatus file : files) {
						LOG.info("Find File " + file.getPath());
						
						dirName = hour.getPath().getName();
						
						Path dest = new Path(result
								+ File.separator + type.getPath().getName()
								+ File.separator + hour.getPath().getName());
						//+ File.separator + file.getPath().getName());
						LOG.info("Dest " + dest.toString());
						
						if (!fs.exists(dest)) {
							fs.mkdirs(dest);
							rename = fs.rename(file.getPath(), new Path(dest+File.separator+file.getPath().getName()));
						}else{
							rename = fs.rename(file.getPath(), new Path(dest+File.separator+file.getPath().getName()));
						}
						
						LOG.info("Moving " + file.getPath() + " to "
								+ new Path(dest+File.separator+file.getPath().getName()).toString() + " "
								+ rename);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return "cleanUp";
	}
}

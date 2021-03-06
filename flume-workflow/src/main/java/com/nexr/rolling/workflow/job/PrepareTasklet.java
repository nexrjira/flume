package com.nexr.rolling.workflow.job;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.framework.workflow.StepContext;
import com.nexr.rolling.workflow.RetryableDFSTaskletSupport;
import com.nexr.rolling.workflow.RollingConstants;

/**
 * 기초 데이터를 넣어주는 작업을 한다.
 * RESULT -> INPUT 으로 경로 변경
 * 
 * @author dani.kim@nexr.com
 */
public class PrepareTasklet extends RetryableDFSTaskletSupport {
	private Logger LOG = LoggerFactory.getLogger(getClass());

	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};
	
	@Override
	public String doRun(StepContext context) {
		Path sourcePath = new Path(context.getConfig().get(RollingConstants.RAW_PATH, null));
		FileStatus[] types = null;
		FileStatus[] timegroups = null;
		try {
			types = fs.listStatus(sourcePath);
			int count = 0;
			for (FileStatus type : types) {
				String input = context.getConfig().get(RollingConstants.INPUT_PATH, null);
				if (!fs.exists(new Path(input, type.getPath().getName()))) {
					fs.mkdirs(new Path(input, type.getPath().getName()));
				}
				timegroups = fs.listStatus(new Path(sourcePath, type.getPath().getName()));
				String isCollectorSource = context.getConfig().get(RollingConstants.IS_COLLECTOR_SOURCE, "false");
				if ("true".equals(isCollectorSource)) {
					for (FileStatus file : timegroups) {
						rename(file.getPath(), String.format("%s/%s/%s", input, type.getPath().getName(), System.currentTimeMillis()));
					}
				} else {
					for (FileStatus file : timegroups) {
						rename(file.getPath(), String.format("%s/%s", input, type.getPath().getName()));
					}
				}
//				if ("true".equals(isCollectorSource)) {
//					count += copyTo(fs.listStatus(type.getPath()), input, type, null);
//				} else {
//					for (FileStatus group : timegroups) {
//						count += copyTo(fs.listStatus(group.getPath()), input, type, group);
//					}
//				}
			}
			if (count == 0) {
				throw new RuntimeException();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return "run";
	}

	private void rename(Path source, String destDir) throws IOException {
		if (!fs.exists(new Path(destDir))) {
			fs.mkdirs(new Path(destDir));
		}
		fs.rename(source, new Path(destDir));
	}

//	private int copyTo(FileStatus[] partials, String input, FileStatus type) throws IOException {
//		int count = 0;
//		for (FileStatus partial : partials) {
//			LOG.info("Find file " + partial.getPath());
//			String destinationFileName = group == null ? type.getPath().getName() : String.format("%s-%s", group.getPath().getName(), partial.getPath().getName());
//			boolean rename = fs.rename(partial.getPath() , new Path(input + File.separator + type.getPath().getName(), destinationFileName));
//			LOG.info("Moving " + partial.toString() + ", status is: " + rename);
//			count++;
//		}
//		return count;
//	}
}

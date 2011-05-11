package com.nexr.rolling.core;

import org.apache.log4j.Logger;

public class RollingConfig {

	private static final Logger log = Logger.getLogger(RollingConfig.class);

	public static final String RAW_PATH = "raw.path";

	public static final String HOURLY_MR_RESULT_PATH = "hourly.mr.result.path";
	public static final String HOURLY_MR_INPUT_PATH = "hourly.mr.input.path";
	public static final String HOURLY_MR_OUTPUT_PATH = "hourly.mr.output.path";

	public static final String DAILY_MR_RESULT_PATH = "daily.mr.result.path";
	public static final String DAILY_MR_INPUT_PATH = "daily.mr.input.path";
	public static final String DAILY_MR_OUTPUT_PATH = "daily.mr.output.path";

	public static final String RETRY_COUNT = "retry.count";
	public static final String RETRY_DELAY = "retry.delay.time";
	public static final String NOTIFY_EMAIL = "notify.email";

	public static final String HOURLY_SCHEDULE = "hourly.schedule";
	public static final String DAILY_SCHEDULE = "daily.schedule";

	private String rawPath;
	private String hourlyMrResultPath;
	private String hourlyMrInputPath;
	private String hourlyMrOutputPath;
	private String dailyMrResultPath;
	private String dailyMrInputPath;
	private String dailyMrOutputPath;
	private int retryCount;
	private long retryDelaytime;
	private String notifyEmail;
	private String hourlySchedule;
	private String dailySchedule;

	public String getRawPath() {
		return rawPath;
	}

	public void setRawPath(String rawPath) {
		this.rawPath = rawPath;
	}

	public String getHourlyMrResultPath() {
		return hourlyMrResultPath;
	}

	public void setHourlyMrResultPath(String hourlyMrResultPath) {
		this.hourlyMrResultPath = hourlyMrResultPath;
	}

	public String getHourlyMrInputPath() {
		return hourlyMrInputPath;
	}

	public void setHourlyMrInputPath(String hourlyMrInputPath) {
		this.hourlyMrInputPath = hourlyMrInputPath;
	}

	public String getHourlyMrOutputPath() {
		return hourlyMrOutputPath;
	}

	public void setHourlyMrOutputPath(String hourlyMrOutputPath) {
		this.hourlyMrOutputPath = hourlyMrOutputPath;
	}

	public String getDailyMrResultPath() {
		return dailyMrResultPath;
	}

	public void setDailyMrResultPath(String dailyMrResultPath) {
		this.dailyMrResultPath = dailyMrResultPath;
	}

	public String getDailyMrInputPath() {
		return dailyMrInputPath;
	}

	public void setDailyMrInputPath(String dailyMrInputPath) {
		this.dailyMrInputPath = dailyMrInputPath;
	}

	public String getDailyMrOutputPath() {
		return dailyMrOutputPath;
	}

	public void setDailyMrOutputPath(String dailyMrOutputPath) {
		this.dailyMrOutputPath = dailyMrOutputPath;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public long getRetryDelaytime() {
		return retryDelaytime;
	}

	public void setRetryDelaytime(long retryDelaytime) {
		this.retryDelaytime = retryDelaytime;
	}

	public String getNotifyEmail() {
		return notifyEmail;
	}

	public void setNotifyEmail(String notifyEmail) {
		this.notifyEmail = notifyEmail;
	}

	public String getHourlySchedule() {
		return hourlySchedule;
	}

	public void setHourlySchedule(String hourlySchedule) {
		this.hourlySchedule = hourlySchedule;
	}

	public String getDailySchedule() {
		return dailySchedule;
	}

	public void setDailySchedule(String dailySchedule) {
		this.dailySchedule = dailySchedule;
	}

	@Override
	public String toString() {
		return "Change RollingConfig [rawPath=" + rawPath
				+ ", hourlyMrResultPath=" + hourlyMrResultPath
				+ ", hourlyMrInputPath=" + hourlyMrInputPath
				+ ", hourlyMrOutputPath=" + hourlyMrOutputPath
				+ ", dailyMrResultPath=" + dailyMrResultPath
				+ ", dailyMrInputPath=" + dailyMrInputPath
				+ ", dailyMrOutputPath=" + dailyMrOutputPath + ", retryCount="
				+ retryCount + ", retryDelaytime=" + retryDelaytime
				+ ", notifyEmail=" + notifyEmail + ", hourlySchedule="
				+ hourlySchedule + ", dailySchedule=" + dailySchedule + "]";
	}

}

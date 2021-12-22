package com.training.statementservice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
@EnableScheduling
public class StatementServiceApplication {

	private final JobLauncher jobLauncher;
	private final Job job;

	public static void main(String[] args) {
		SpringApplication.run(StatementServiceApplication.class, args);
	}

	@Scheduled(cron = "0 */1 * * * ?")
	public void runJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
		JobParameters params = new JobParametersBuilder()
				.addString("JobId", String.valueOf(System.currentTimeMillis())).toJobParameters();
		jobLauncher.run(job,params);
	}


}

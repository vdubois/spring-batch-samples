package io.github.com;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Date;

@SpringBootApplication
public class SpringBatchSampleCompositeWriterApplication {

	public static void main(String[] args) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
		ConfigurableApplicationContext ctx = SpringApplication.run(SpringBatchSampleCompositeWriterApplication.class, args);
		JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
		JobParameters jobParameters =
				new JobParametersBuilder()
						.addDate("date", new Date())
						.addString("outputFileDelimited", "sample-written-delimited-data.csv")
						.addString("outputFileFixed", "sample-written-fixed-data.csv").toJobParameters();
		jobLauncher.run(ctx.getBean(Job.class), jobParameters);
		ctx.close();
	}
}

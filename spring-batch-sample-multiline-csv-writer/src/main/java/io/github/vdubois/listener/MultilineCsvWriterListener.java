package io.github.vdubois.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by vdubois on 25/11/16.
 */
//@Component
public class MultilineCsvWriterListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(MultilineCsvWriterListener.class);

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("**************************************************");
            log.info("=> !!! JOB FINISHED! Time to verify the results");
            ClassPathResource writedFile = new ClassPathResource("multiline-writed-data.csv");
            try {
                Stream<String> lines = Files.lines(Paths.get(writedFile.getURI()));
                lines.forEach(log::info);
            } catch (IOException ioException) {
                log.error(ioException.getMessage(), ioException);
            }
            log.info("**************************************************");
        }
    }
}

package io.github.vdubois.listener;

import io.github.vdubois.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by vdubois on 22/11/16.
 */
@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        jdbcTemplate.execute("DELETE FROM users");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("**************************************************");
            log.info("=> !!! JOB FINISHED! Time to verify the results");

            List<User> results = jdbcTemplate.query("SELECT * FROM users", (resultSet, row) -> {
                User user = new User();
                user.setId("" + resultSet.getInt(1));
                user.setFullName(resultSet.getString(2));
                user.setPosition(resultSet.getString(3));
                user.setCompanyNumber(resultSet.getString(4));
                return user;
            });

            results.forEach(user -> log.info("Found <" + user + "> in the database."));
            log.info("**************************************************");
            jdbcTemplate.execute("DELETE FROM users");
        }
    }
}

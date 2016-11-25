package io.github.vdubois.config;

import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class JDBCReaderConfiguration {

    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        return simpleJobLauncher;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<User> reader(DataSource dataSource, @Value(value = "#{jobParameters['position']}") String position) throws Exception {
        JdbcPagingItemReader<User> itemReader = new JdbcPagingItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setQueryProvider(queryProvider(dataSource));
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("position", position);
        itemReader.setParameterValues(parameters);
        itemReader.setRowMapper(new BeanPropertyRowMapper<>(User.class));
        itemReader.setPageSize(10);
        return itemReader;
    }

    private PagingQueryProvider queryProvider(DataSource dataSource) throws Exception {
        SqlPagingQueryProviderFactoryBean queryProviderFactoryBean = new SqlPagingQueryProviderFactoryBean();
        queryProviderFactoryBean.setDataSource(dataSource);
        queryProviderFactoryBean.setSelectClause("select id, name, position, companyNumber");
        queryProviderFactoryBean.setFromClause("from users");
        queryProviderFactoryBean.setWhereClause("where position = :position");
        queryProviderFactoryBean.setSortKey("id");
        return queryProviderFactoryBean.getObject();
    }

    @Bean
    public Job jdbcReaderJob(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("jdbcReaderJob")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, JdbcPagingItemReader<User> reader, ItemWriter logWriter) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader)
                .writer(logWriter)
                .build();
    }
}

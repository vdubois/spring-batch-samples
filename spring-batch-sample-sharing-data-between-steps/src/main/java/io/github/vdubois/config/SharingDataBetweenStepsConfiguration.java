package io.github.vdubois.config;

import io.github.vdubois.model.User;
import io.github.vdubois.tasklet.CalculatePositionTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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
public class SharingDataBetweenStepsConfiguration {

    @Bean
    @StepScope
    public JdbcPagingItemReader<User> jdbcReader(DataSource dataSource, @Value(value = "#{jobExecutionContext['position']}") String position) throws Exception {
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
    public Job jdbcReaderJob(JobBuilderFactory jobBuilderFactory, Step calculatePositionStep, Step step) {
        return jobBuilderFactory.get("jdbcReaderJob")
                .incrementer(new RunIdIncrementer())
                .start(calculatePositionStep)
                .next(step)
                .build();
    }

    @Bean
    public Step calculatePositionStep(StepBuilderFactory stepBuilderFactory, CalculatePositionTasklet tasklet) {
        return stepBuilderFactory.get("calculatePositionStep")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, JdbcPagingItemReader<User> jdbcReader, ItemWriter logWriter) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(jdbcReader)
                .writer(logWriter)
                .build();
    }
}

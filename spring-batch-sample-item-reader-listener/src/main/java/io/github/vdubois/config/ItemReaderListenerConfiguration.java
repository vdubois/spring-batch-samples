package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.listener.ReadUserListener;
import io.github.vdubois.model.User;
import io.github.vdubois.writer.UserJdbcItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class ItemReaderListenerConfiguration {

    @Bean
    public FlatFileItemReader<User> reader() {
        FlatFileItemReader<User> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource("sample-data.csv"));
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<User> lineMapper() {
        DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer());
        lineMapper.setFieldSetMapper(fieldSetMapper());
        return lineMapper;
    }

    private LineTokenizer lineTokenizer() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"id", "fullName", "position", "companyNumber"});
        return lineTokenizer;
    }

    private FieldSetMapper<User> fieldSetMapper() {
        BeanWrapperFieldSetMapper<User> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(User.class);
        return mapper;
    }

    @Bean
    public UserJdbcItemWriter writer(DataSource dataSource) {
        return new UserJdbcItemWriter(dataSource);
    }

    @Bean
    public Job itemReaderListenerJob(JobCompletionNotificationListener jobCompletionNotificationListener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("itemReaderListenerJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(ReadUserListener itemReadListener, StepBuilderFactory stepBuilderFactory, UserJdbcItemWriter writer) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader())
                .writer(writer)
                .listener(itemReadListener)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(1000)
                .build();
    }
}

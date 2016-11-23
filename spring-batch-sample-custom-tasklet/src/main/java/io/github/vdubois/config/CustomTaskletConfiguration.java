package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.model.User;
import io.github.vdubois.tasklet.DecompressTasklet;
import io.github.vdubois.writer.UserJdbcItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
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
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.File;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class CustomTaskletConfiguration {

    @Bean
    public FlatFileItemReader<User> reader() {
        FlatFileItemReader<User> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(new File(System.getProperty("java.io.tmpdir"), "sample-data.csv")));
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
        lineTokenizer.setNames(new String[]{"id", "fullName", "position", "companyNumber"});
        return lineTokenizer;
    }

    private FieldSetMapper<User> fieldSetMapper() {
        BeanWrapperFieldSetMapper<User> mapper = new BeanWrapperFieldSetMapper<User>();
        mapper.setTargetType(User.class);
        return mapper;
    }

    @Bean
    public UserJdbcItemWriter writer(DataSource dataSource) {
        return new UserJdbcItemWriter(dataSource);
    }

    @Bean
    public Job csvReaderJob(JobCompletionNotificationListener jobCompletionNotificationListener, JobBuilderFactory jobBuilderFactory, Step stepWithCustomTasklet, Step step) {
        return jobBuilderFactory.get("customTaskletJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .start(stepWithCustomTasklet)
                .next(step)
                .build();
    }

    @Bean
    public Step stepWithCustomTasklet(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("stepWithCustomTasklet")
                .tasklet(tasklet())
                .build();
    }

    @Bean
    public Tasklet tasklet() {
        DecompressTasklet decompressTasklet = new DecompressTasklet();
        decompressTasklet.setInputFile(new ClassPathResource("sample-data.zip"));
        decompressTasklet.setTargetDirectory(System.getProperty("java.io.tmpdir"));
        decompressTasklet.setTargetFile("sample-data.csv");
        return decompressTasklet;
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, UserJdbcItemWriter writer) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader())
                .writer(writer)
                .build();
    }
}

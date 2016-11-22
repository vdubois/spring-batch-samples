package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.model.User;
import io.github.vdubois.tasklet.DecompressTasklet;
import io.github.vdubois.writer.UserJdbcItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.File;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@PropertySource(value = "classpath:batch.properties", encoding = "UTF-8")
@EnableBatchProcessing
public class JobParametersValidatorConfiguration {

    @Value("${database.url}")
    private String databaseUrl;

    @Value("${database.user}")
    private String databaseUser;

    @Value("${database.password}")
    private String databasePassword;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public DataSource dataSource() {
        return new DriverManagerDataSource(databaseUrl, databaseUser, databasePassword);
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }

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
        lineTokenizer.setNames(new String[] {"id", "fullName", "position", "companyNumber"});
        return lineTokenizer;
    }

    private FieldSetMapper<User> fieldSetMapper() {
        BeanWrapperFieldSetMapper<User> mapper = new BeanWrapperFieldSetMapper<User>();
        mapper.setTargetType(User.class);
        return mapper;
    }

    @Bean
    public UserJdbcItemWriter writer() {
        return new UserJdbcItemWriter(dataSource());
    }

    @Bean
    public Job csvReaderJob(JobCompletionNotificationListener jobCompletionNotificationListener) {
        return jobBuilderFactory.get("customTaskletJob")
                .incrementer(new RunIdIncrementer())
                .validator(validator())
                .listener(jobCompletionNotificationListener)
                .start(stepWithCustomTasklet())
                .next(step())
                .build();
    }

    @Bean
    public JobParametersValidator validator() {
        DefaultJobParametersValidator parametersValidator = new DefaultJobParametersValidator();
        parametersValidator.setRequiredKeys(new String[] {"inputFile", "targetDirectory", "targetFile"});
        return parametersValidator;
    }

    @Bean
    public Step stepWithCustomTasklet() {
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
    public Step step() {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader())
                .writer(writer())
                .build();
    }
}

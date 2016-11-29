package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.model.User;
import io.github.vdubois.service.UserService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.adapter.PropertyExtractingDelegatingItemWriter;
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

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class ServiceWithParametersWriterConfiguration {

    @Bean
    public FlatFileItemReader<User> csvReader() {
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
    public PropertyExtractingDelegatingItemWriter<User> serviceWithParametersWriter(UserService userService) {
        PropertyExtractingDelegatingItemWriter<User> writer = new PropertyExtractingDelegatingItemWriter<>();
        writer.setTargetObject(userService);
        writer.setTargetMethod("saveWithArgs");
        writer.setFieldsUsedAsTargetMethodArguments(new String[]{"id", "name", "position", "companyNumber"});
        return writer;
    }

    @Bean
    public Job serviceWithParametersWriterJob(JobCompletionNotificationListener jobCompletionNotificationListener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("serviceWithParametersWriterJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, PropertyExtractingDelegatingItemWriter<User> serviceWithParametersWriter, FlatFileItemReader<User> csvReader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(csvReader)
                .writer(serviceWithParametersWriter)
                .build();
    }
}

package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.model.MailingList;
import io.github.vdubois.model.Resource;
import io.github.vdubois.model.User;
import io.github.vdubois.writer.ResourceJdbcItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class MultilineCsvReaderConfiguration {

    @Bean
    public FlatFileItemReader<Resource> reader() {
        FlatFileItemReader<Resource> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource("multiline-sample-data.csv"));
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Resource> lineMapper() {
        PatternMatchingCompositeLineMapper<Resource> lineMapper = new PatternMatchingCompositeLineMapper<>();
        lineMapper.setTokenizers(tokenizers());
        lineMapper.setFieldSetMappers(fieldSetMappers());
        return lineMapper;
    }

    private Map<String, FieldSetMapper<Resource>> fieldSetMappers() {
        Map<String, FieldSetMapper<Resource>> mappers = new HashMap<>();
        mappers.put("USR*", userFieldSetMapper());
        mappers.put("ML*", mailingListFieldSetMapper());
        return mappers;
    }

    private Map<String, LineTokenizer> tokenizers() {
        Map<String, LineTokenizer> tokenizers = new HashMap<>();
        tokenizers.put("USR*", userLineTokenizer());
        tokenizers.put("ML*", mailingListLineTokenizer());
        return tokenizers;
    }

    private LineTokenizer userLineTokenizer() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"id", "id", "fullName", "position", "companyNumber"});
        return lineTokenizer;
    }

    private LineTokenizer mailingListLineTokenizer() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] {"id", "id", "email"});
        return lineTokenizer;
    }

    private FieldSetMapper<Resource> userFieldSetMapper() {
        BeanWrapperFieldSetMapper<Resource> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(User.class);
        return mapper;
    }

    private FieldSetMapper<Resource> mailingListFieldSetMapper() {
        BeanWrapperFieldSetMapper<Resource> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(MailingList.class);
        return mapper;
    }

    @Bean
    public ItemWriter<Resource> writer(DataSource dataSource) {
        return new ResourceJdbcItemWriter(dataSource);
    }

    @Bean
    public Job multilineCsvReaderJob(JobCompletionNotificationListener jobCompletionNotificationListener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("multilineCsvReaderJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, ItemWriter<Resource> writer, FlatFileItemReader<Resource> reader) {
        return stepBuilderFactory.get("step")
                .<Resource, Resource>chunk(2)
                .reader(reader)
                .writer(writer)
                .build();
    }
}

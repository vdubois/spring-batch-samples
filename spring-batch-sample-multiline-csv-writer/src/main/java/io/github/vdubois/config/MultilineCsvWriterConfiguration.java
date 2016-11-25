package io.github.vdubois.config;

import io.github.vdubois.aggregator.ResourceLineAggregator;
import io.github.vdubois.listener.MultilineCsvWriterListener;
import io.github.vdubois.model.MailingList;
import io.github.vdubois.model.Resource;
import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@EnableBatchProcessing
public class MultilineCsvWriterConfiguration {

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
    public ItemWriter<Resource> writer() {
        FlatFileItemWriter<Resource> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource("multiline-sample-written-data.csv"));
        itemWriter.setAppendAllowed(true);
        itemWriter.setLineAggregator(resourceLineAggregator());
        return itemWriter;
    }

    private ResourceLineAggregator resourceLineAggregator() {
        ResourceLineAggregator lineAggregator = new ResourceLineAggregator();
        Map<Class, LineAggregator<Resource>> aggregators = new HashMap<>();
        aggregators.put(User.class, userAggregator());
        aggregators.put(MailingList.class, mailingListAggregator());
        lineAggregator.setAggregators(aggregators);
        return lineAggregator;
    }

    private FormatterLineAggregator<Resource> userAggregator() {
        FormatterLineAggregator<Resource> lineAggregator = new FormatterLineAggregator<>();
        BeanWrapperFieldExtractor<Resource> userFieldExtractor = new BeanWrapperFieldExtractor<>();
        userFieldExtractor.setNames(new String[] {"name", "position"});
        lineAggregator.setFieldExtractor(userFieldExtractor);
        lineAggregator.setFormat("YOU-ZER,%s,%s");
        return lineAggregator;
    }

    private FormatterLineAggregator<Resource> mailingListAggregator() {
        FormatterLineAggregator<Resource> lineAggregator = new FormatterLineAggregator<>();
        BeanWrapperFieldExtractor<Resource> userFieldExtractor = new BeanWrapperFieldExtractor<>();
        userFieldExtractor.setNames(new String[] {"email"});
        lineAggregator.setFieldExtractor(userFieldExtractor);
        lineAggregator.setFormat("MAILING-LIST,%s");
        return lineAggregator;
    }

    @Bean
    public Job multilineCsvReaderJob(MultilineCsvWriterListener listener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("multilineCsvReaderJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
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

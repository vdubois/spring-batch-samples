package io.github.com.config;

import io.github.vdubois.config.InfrastructureConfiguration;
import io.github.vdubois.listener.OutputFileListener;
import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.util.Arrays;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class CompositeWriterConfiguration {

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
    @StepScope
    public CompositeItemWriter<User> compositeWriter(FlatFileItemWriter<User> delimitedWriter, FlatFileItemWriter<User> fixedWriter) {
        CompositeItemWriter<User> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(delimitedWriter, fixedWriter));
        return writer;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<User> delimitedWriter(@Value("#{jobParameters['outputFileDelimited']}") String outputFile) {
        FlatFileItemWriter<User> delimitedWriter = new FlatFileItemWriter<>();
        delimitedWriter.setResource(new FileSystemResource(outputFile));
        delimitedWriter.setAppendAllowed(false);
        delimitedWriter.setLineAggregator(delimitedLineAggregator());
        return delimitedWriter;
    }

    private LineAggregator<User> delimitedLineAggregator() {
        DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor());
        return lineAggregator;
    }

    private BeanWrapperFieldExtractor<User> fieldExtractor() {
        BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "position", "companyNumber"});
        return fieldExtractor;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<User> fixedWriter(@Value("#{jobParameters['outputFileFixed']}") String outputFile) {
        FlatFileItemWriter<User> fixedWriter = new FlatFileItemWriter<>();
        fixedWriter.setResource(new FileSystemResource(outputFile));
        fixedWriter.setAppendAllowed(false);
        fixedWriter.setLineAggregator(formatterLineAggregator());
        return fixedWriter;
    }

    private LineAggregator<User> formatterLineAggregator() {
        FormatterLineAggregator<User> lineAggregator = new FormatterLineAggregator<>();
        lineAggregator.setFieldExtractor(fieldExtractor());
        lineAggregator.setFormat("%-4s%-30s%-30s%-10s");
        return lineAggregator;
    }

    @Bean
    public OutputFileListener outputFileListener() {
        return new OutputFileListener();
    }

    @Bean
    public Job compositeWriterJob(OutputFileListener outputFileListener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("compositeWriterJob")
                .incrementer(new RunIdIncrementer())
                .listener(outputFileListener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, CompositeItemWriter<User> compositeWriter, FlatFileItemReader<User> csvReader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(csvReader)
                .writer(compositeWriter)
                .build();
    }
}

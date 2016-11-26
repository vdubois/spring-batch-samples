package io.github.vdubois.config;

import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.SimpleResourceSuffixCreator;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;

import java.nio.charset.StandardCharsets;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class MultifileCsvWriterConfiguration {

    @Bean
    @StepScope
    public MultiResourceItemWriter writer(@Value("#{jobParameters['outputFile']}") String outputFile) {
        MultiResourceItemWriter writer = new MultiResourceItemWriter();
        writer.setItemCountLimitPerResource(2);
        writer.setResource(new FileSystemResource(outputFile));
        writer.setResourceSuffixCreator(new SimpleResourceSuffixCreator());
        writer.setDelegate(fileItemWriter());
        writer.setSaveState(true);
        return writer;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<User> fileItemWriter() {
        FlatFileItemWriter<User> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setLineAggregator(lineAggregator());
        itemWriter.setShouldDeleteIfExists(true);
        itemWriter.setEncoding(StandardCharsets.UTF_8.name());
        return itemWriter;
    }

    private LineAggregator<User> lineAggregator() {
        DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "position", "companyNumber"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        return lineAggregator;
    }

    @Bean
    public Job multifileCsvWriterJob(/*OutputFileListener listener, */JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("multifileCsvWriterJob")
                .incrementer(new RunIdIncrementer())
//                .listener(listener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, MultiResourceItemWriter writer, JdbcCursorItemReader<User> reader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader)
                .writer(writer)
                .build();
    }
}

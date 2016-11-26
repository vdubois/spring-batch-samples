package io.github.vdubois.config;

import io.github.vdubois.callback.FormatterCallback;
import io.github.vdubois.listener.OutputFileListener;
import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class HeaderAndFooterCsvWriterConfiguration {

    @Bean
    @StepScope
    public FlatFileItemWriter<User> writer(@Value("#{jobParameters['outputFile']}") String outputFile) {
        FlatFileItemWriter<User> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource(outputFile));
        itemWriter.setAppendAllowed(true);
        itemWriter.setLineAggregator(lineAggregator());
        itemWriter.setHeaderCallback(headerCallback());
        itemWriter.setFooterCallback(footerCallback());
        return itemWriter;
    }

    private LineAggregator<User> lineAggregator() {
        DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] { "id", "name", "position", "companyNumber" });
        lineAggregator.setFieldExtractor(fieldExtractor);
        return lineAggregator;
    }

    private FlatFileHeaderCallback headerCallback() {
        FormatterCallback headerCallback = new FormatterCallback();
        headerCallback.setFormat("%1$s,%2$s%n%3$s");
        Date dayDate = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Europe/Paris"))).getTime();
        SimpleDateFormat dayFormat = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat hoursFormat = new SimpleDateFormat("HHmm");
        headerCallback.setParameters(new String[]{dayFormat.format(dayDate), hoursFormat.format(dayDate), "usersexport"});
        headerCallback.setGenerateBom(true);
        return headerCallback;
    }

    private FlatFileFooterCallback footerCallback() {
        FormatterCallback footerCallback = new FormatterCallback();
        footerCallback.setFormat("&lt;END_OF_FILE&gt;");
        return null;
    }

    @Bean
    public Job headerAndFooterCsvWriterJob(JobBuilderFactory jobBuilderFactory, Step step, OutputFileListener listener) {
        return jobBuilderFactory.get("headerAndFooterCsvWriterJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, FlatFileItemWriter<User> writer, JdbcCursorItemReader<User> reader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader)
                .writer(writer)
                .build();
    }
}

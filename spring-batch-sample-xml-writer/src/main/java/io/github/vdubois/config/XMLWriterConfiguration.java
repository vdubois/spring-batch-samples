package io.github.vdubois.config;

import io.github.vdubois.listener.OutputFileListener;
import io.github.vdubois.model.User;
import io.github.vdubois.tasklet.XMLPrettifyTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class XMLWriterConfiguration {

    @Bean
    @StepScope
    public StaxEventItemWriter<User> writer(@Value("#{jobParameters['outputFile']}") String outputFile) {
        StaxEventItemWriter<User> itemWriter = new StaxEventItemWriter<>();
        itemWriter.setResource(new FileSystemResource(outputFile));
        itemWriter.setRootTagName("users");
        itemWriter.setMarshaller(marshaller());
        itemWriter.setOverwriteOutput(true);
        return itemWriter;
    }

    private XStreamMarshaller marshaller() {
        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("user", User.class);
        marshaller.setAliases(aliases);
        Map<Class<?>, String> omittedFields = new HashMap<>();
        omittedFields.put(User.class, "id");
        marshaller.setOmittedFields(omittedFields);
        return marshaller;
    }

    @Bean
    @StepScope
    public XMLPrettifyTasklet tasklet(@Value("#{jobParameters['outputFile']}") String outputFile) {
        XMLPrettifyTasklet tasklet = new XMLPrettifyTasklet();
        tasklet.setInputFile(new FileSystemResource(outputFile));
        return tasklet;
    }

    @Bean
    public Job xmlWriterJob(JobBuilderFactory jobBuilderFactory, Step step, Step xmlPrettifyStep, OutputFileListener listener) {
        return jobBuilderFactory.get("xmlWriterJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step)
                .next(xmlPrettifyStep)
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, StaxEventItemWriter<User> writer, JdbcCursorItemReader<User> reader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public Step xmlPrettifyStep(StepBuilderFactory stepBuilderFactory, XMLPrettifyTasklet tasklet) {
        return stepBuilderFactory.get("xmlPrettifyStep")
                .tasklet(tasklet)
                .build();
    }
}

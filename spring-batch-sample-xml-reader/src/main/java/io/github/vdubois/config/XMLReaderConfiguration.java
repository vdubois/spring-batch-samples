package io.github.vdubois.config;

import io.github.vdubois.listener.JobCompletionNotificationListener;
import io.github.vdubois.model.User;
import io.github.vdubois.writer.UserJdbcItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class XMLReaderConfiguration {

    @Bean
    public StaxEventItemReader<User> reader() {
        StaxEventItemReader<User> itemReader = new StaxEventItemReader<>();
        itemReader.setResource(new ClassPathResource("xmlreader.xml"));
        itemReader.setFragmentRootElementName("user");
        itemReader.setUnmarshaller(unmarshaller());
        return itemReader;
    }

    private XStreamMarshaller unmarshaller() {
        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user", "io.github.vdubois.model.User");
        marshaller.setAliases(aliases);
        return marshaller;
    }

    @Bean
    public UserJdbcItemWriter writer(DataSource dataSource) {
        return new UserJdbcItemWriter(dataSource);
    }

    @Bean
    public Job xmlReaderJob(JobCompletionNotificationListener jobCompletionNotificationListener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("xmlReaderJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step)
                .end()
                .build();
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

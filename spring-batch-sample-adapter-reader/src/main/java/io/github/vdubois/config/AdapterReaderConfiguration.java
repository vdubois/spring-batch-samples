package io.github.vdubois.config;

import io.github.vdubois.adapter.UserRepositoryAdapter;
import io.github.vdubois.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@Import({InfrastructureConfiguration.class})
public class AdapterReaderConfiguration {

    @Bean
    public ItemReaderAdapter<User> reader(UserRepositoryAdapter userRepositoryAdapter) throws Exception {
        ItemReaderAdapter<User> itemReader = new ItemReaderAdapter<>();
        itemReader.setTargetObject(userRepositoryAdapter);
        itemReader.setTargetMethod("nextUserWithPosition");
        itemReader.setArguments(new Object[] {"Technical Expert"});
        return itemReader;
    }

    @Bean
    public Job adapterReaderJob(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("adapterReaderJob")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, ItemReaderAdapter<User> reader, ItemWriter logWriter) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(reader)
                .writer(logWriter)
                .build();
    }
}

package io.github.vdubois.config;

import io.github.vdubois.classifier.delegate.UserOperationClassifier;
import io.github.vdubois.classifier.model.User;
import io.github.vdubois.listener.JobCompletionNotificationListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.classify.BackToBackPatternClassifier;
import org.springframework.classify.Classifier;
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
public class ClassifierCompositeWriterConfiguration {

    @Bean
    public FlatFileItemReader<User> csvReader() {
        FlatFileItemReader<User> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource("classifier-sample-data.csv"));
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
        lineTokenizer.setNames(new String[] {"id", "fullName", "position", "companyNumber", "operation"});
        return lineTokenizer;
    }

    private FieldSetMapper<User> fieldSetMapper() {
        BeanWrapperFieldSetMapper<User> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(User.class);
        return mapper;
    }

    @Bean
    public ClassifierCompositeItemWriter<User> compositeWriter(Classifier<User, ItemWriter<? super User>> classifier) {
        ClassifierCompositeItemWriter<User> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(classifier);
        return writer;
    }

    @Bean
    public Classifier<User, ItemWriter<? super User>> classifier(UserOperationClassifier userClassifier, Map matcherMap) {
        BackToBackPatternClassifier classifier = new BackToBackPatternClassifier();
        classifier.setRouterDelegate(userClassifier);
        classifier.setMatcherMap(matcherMap);
        return classifier;
    }

    @Bean
    public Map matcherMap(JdbcBatchItemWriter<User> addUserWriter, JdbcBatchItemWriter<User> updateUserWriter, JdbcBatchItemWriter<User> deleteUserWriter) {
        Map<String, JdbcBatchItemWriter<User>> matcherMap = new HashMap<>();
        matcherMap.put("A", addUserWriter);
        matcherMap.put("U", updateUserWriter);
        matcherMap.put("D", deleteUserWriter);
        return matcherMap;
    }

    @Bean
    public JdbcBatchItemWriter<User> addUserWriter(DataSource dataSource) {
        JdbcBatchItemWriter<User> addUserWriter = new JdbcBatchItemWriter<>();
        addUserWriter.setDataSource(dataSource);
        addUserWriter.setAssertUpdates(true);
        addUserWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        addUserWriter.setSql("insert into users (id, name, position, companyNumber) values (:id, :name, :position, :companyNumber)");
        return addUserWriter;
    }

    @Bean
    public JdbcBatchItemWriter<User> updateUserWriter(DataSource dataSource) {
        JdbcBatchItemWriter<User> updateUserWriter = new JdbcBatchItemWriter<>();
        updateUserWriter.setDataSource(dataSource);
        updateUserWriter.setAssertUpdates(true);
        updateUserWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        updateUserWriter.setSql("update users set name = :name, position = :position, companyNumber = :companyNumber where id = :id");
        return updateUserWriter;
    }

    @Bean
    public JdbcBatchItemWriter<User> deleteUserWriter(DataSource dataSource) {
        JdbcBatchItemWriter<User> deleteUserWriter = new JdbcBatchItemWriter<>();
        deleteUserWriter.setDataSource(dataSource);
        deleteUserWriter.setAssertUpdates(true);
        deleteUserWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        deleteUserWriter.setSql("delete from users where id = :id");
        return deleteUserWriter;
    }

    @Bean
    public Job compositeWriterJob(JobCompletionNotificationListener listener, JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("compositeWriterJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, ClassifierCompositeItemWriter<User> compositeWriter, FlatFileItemReader<User> csvReader) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(csvReader)
                .writer(compositeWriter)
                .build();
    }
}

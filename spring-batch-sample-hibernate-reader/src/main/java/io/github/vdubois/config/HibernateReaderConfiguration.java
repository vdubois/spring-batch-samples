package io.github.vdubois.config;

import io.github.vdubois.model.jpa.User;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import javax.naming.NamingException;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by vdubois on 21/11/16.
 */
@Configuration
@EnableBatchProcessing
public class HibernateReaderConfiguration {

    @Value("${database.url}")
    private String databaseUrl;

    @Value("${database.user}")
    private String databaseUser;

    @Value("${database.password}")
    private String databasePassword;

    @Bean
    @Primary
    public DataSource myDataSource() {
        return new DriverManagerDataSource(databaseUrl, databaseUser, databasePassword);
    }

    @Bean
    public JpaPagingItemReader<User> jpaReader(EntityManagerFactory entityManagerFactory) throws Exception {
        JpaPagingItemReader<User> itemReader = new JpaPagingItemReader<>();
        itemReader.setEntityManagerFactory(entityManagerFactory);
        itemReader.setQueryString("from User");
        return itemReader;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource myDataSource) throws SQLException, NamingException {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setPackagesToScan("io.github.vdubois.model.jpa");
        em.setDataSource(myDataSource);
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        em.setPersistenceProviderClass(HibernatePersistenceProvider.class);
        em.setJpaProperties(hibernateProperties());
        return em;
    }

    Properties hibernateProperties() {
        return new Properties() {
            {
                setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");
                setProperty("hibernate.show_sql", "true");
            }
        };
    }

    @Bean
    public Job hibernateReaderJob(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("hibernateReaderJob")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, JpaPagingItemReader<User> jpaReader, ItemWriter logWriter) {
        return stepBuilderFactory.get("step")
                .<User, User>chunk(2)
                .reader(jpaReader)
                .writer(logWriter)
                .build();
    }
}

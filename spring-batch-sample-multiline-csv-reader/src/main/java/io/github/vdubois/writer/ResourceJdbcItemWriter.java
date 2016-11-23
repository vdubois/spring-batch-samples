package io.github.vdubois.writer;

import io.github.vdubois.model.MailingList;
import io.github.vdubois.model.Resource;
import io.github.vdubois.model.User;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

/**
 * Created by vdubois on 23/11/16.
 */
public class ResourceJdbcItemWriter implements ItemWriter<Resource> {

    private static final String INSERT_USER = "insert into users (id, name, position, companyNumber) values (?, ?, ?, ?)";

    private static final String UPDATE_USER = "update users set name=?, position=?, companyNumber=? where id=?";

    private static final String INSERT_MAILING_LIST = "insert into mailingList (id, email) values (?, ?)";

    private static final String UPDATE_MAILING_LIST = "update mailingList set email=? where id=?";

    private JdbcTemplate jdbcTemplate;

    public ResourceJdbcItemWriter(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /**
     * Process the supplied data element. Will not be called with any null items
     * in normal operation.
     *
     * @param items items to write
     * @throws Exception if there are errors. The framework will catch the
     *                   exception and convert or rethrow it as appropriate.
     */
    @Override
    public void write(List<? extends Resource> items) throws Exception {
        for (Resource resource : items) {
            if (resource.getClass().isAssignableFrom(User.class)) {
                User user = (User) resource;
                int updated = jdbcTemplate.update(UPDATE_USER,
                        user.getFullName(), user.getPosition(), user.getCompanyNumber(), user.getId());
                if (updated == 0) {
                    jdbcTemplate.update(INSERT_USER,
                            user.getId(), user.getFullName(), user.getPosition(), user.getCompanyNumber());
                }
            } else if (resource.getClass().isAssignableFrom(MailingList.class)) {
                MailingList mailingList = (MailingList) resource;
                int updated = jdbcTemplate.update(UPDATE_MAILING_LIST,
                        mailingList.getEmail(), mailingList.getId());
                if (updated == 0) {
                    jdbcTemplate.update(INSERT_MAILING_LIST,
                            mailingList.getId(), mailingList.getEmail());
                }
            }
        }
    }
}
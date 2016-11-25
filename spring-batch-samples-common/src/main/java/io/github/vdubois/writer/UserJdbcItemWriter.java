package io.github.vdubois.writer;

import io.github.vdubois.model.User;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

/**
 * Created by vdubois on 21/11/16.
 */
public class UserJdbcItemWriter implements ItemWriter<User> {

    private static final String INSERT_USER = "insert into users (id, name, position, companyNumber) values (?, ?, ?, ?)";

    private static final String UPDATE_USER = "update users set name=?, position=?, companyNumber=? where id=?";

    private JdbcTemplate jdbcTemplate;

    public UserJdbcItemWriter(DataSource dataSource) {
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
    public void write(List<? extends User> items) throws Exception {
        items.forEach(user -> {
            // we try first to update data
            int updated = jdbcTemplate.update(UPDATE_USER,
                    user.getName(), user.getPosition(), user.getCompanyNumber(), user.getId());
            // if no line has been updated, then user has to be created
            if (updated == 0) {
                jdbcTemplate.update(INSERT_USER,
                        user.getId(), user.getName(), user.getPosition(), user.getCompanyNumber());
            }
        });
    }
}
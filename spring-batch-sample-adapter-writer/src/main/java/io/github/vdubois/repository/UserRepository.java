package io.github.vdubois.repository;

import io.github.vdubois.model.User;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Created by vdubois on 28/11/16.
 */
@Repository
public class UserRepository {

    private static final String INSERT_USER = "insert into users (id, name, position, companyNumber) values (?, ?, ?, ?)";

    private static final String UPDATE_USER = "update users set name=?, position=?, companyNumber=? where id=?";

    private JdbcTemplate jdbcTemplate;

    public UserRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(User user) {
        int updated = jdbcTemplate.update(UPDATE_USER,
                user.getName(), user.getPosition(), user.getCompanyNumber(), user.getId());
        if (updated == 0) {
            jdbcTemplate.update(INSERT_USER,
                    user.getId(), user.getName(), user.getPosition(), user.getCompanyNumber());
        }
    }

}

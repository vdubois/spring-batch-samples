package io.github.vdubois.repository;

import io.github.vdubois.model.User;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by vdubois on 25/11/16.
 */
@Repository
public class UserRepository {

    private static final String QUERY = "select * from users where position = ?";

    private JdbcTemplate jdbcTemplate;

    public UserRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<User> findAllWherePositionEqualTo(String position) {
        return jdbcTemplate.query(QUERY, new Object[]{position}, new BeanPropertyRowMapper<>(User.class));
    }
}

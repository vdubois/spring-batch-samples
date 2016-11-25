package io.github.vdubois.adapter;

import io.github.vdubois.model.User;
import io.github.vdubois.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by vdubois on 25/11/16.
 */
@Service
public class UserRepositoryAdapter {

    private UserRepository userRepository;

    private List<User> users = null;

    public UserRepositoryAdapter(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public User nextUserWithPosition(String position) {
        if (users == null) {
            users = userRepository.findAllWherePositionEqualTo(position);
        }
        if (!users.isEmpty()) {
            return users.remove(0);
        } else {
            return null;
        }
    }
}

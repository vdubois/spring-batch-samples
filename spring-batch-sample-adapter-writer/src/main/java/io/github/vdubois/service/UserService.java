package io.github.vdubois.service;

import io.github.vdubois.model.User;
import io.github.vdubois.repository.UserRepository;
import org.springframework.stereotype.Service;

/**
 * Created by vdubois on 28/11/16.
 */
@Service
public class UserService {

    private UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(User user) {
        userRepository.save(user);
    }
}

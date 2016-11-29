package io.github.vdubois.service;

import io.github.vdubois.model.User;
import io.github.vdubois.repository.UserRepository;
import org.springframework.stereotype.Service;

/**
 * Created by vdubois on 29/11/16.
 */
@Service
public class UserService {

    private UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void saveWithArgs(String id, String name, String position, String number) {
        User user = new User();
        user.setId(id);
        user.setName(name);
        user.setCompanyNumber(number);
        user.setPosition(position);
        userRepository.save(user);
    }
}

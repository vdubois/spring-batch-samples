package io.github.vdubois.validator;

import io.github.vdubois.model.jpa.User;
import lombok.extern.java.Log;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.util.Arrays;
import java.util.List;

/**
 * Created by vdubois on 01/12/16.
 */
@Log
public class JobPositionValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return User.class.equals(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        User user = (User) target;
        List<String> allValidJobPositions = Arrays.asList("Technical Expert");
        if (!allValidJobPositions.contains(user.getPosition())) {
            log.info("Job position ".concat(user.getPosition()).concat(" is not authorized"));
            errors.rejectValue("position", "bad.position", "Job position not authorized");
        }
    }
}

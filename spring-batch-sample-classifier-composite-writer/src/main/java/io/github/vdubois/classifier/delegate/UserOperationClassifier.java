package io.github.vdubois.classifier.delegate;

import io.github.vdubois.classifier.model.User;
import org.springframework.batch.support.annotation.Classifier;
import org.springframework.stereotype.Service;

/**
 * Created by vdubois on 29/11/16.
 */
@Service
public class UserOperationClassifier {
    @Classifier
    public String classify(User classifiable) {
        return classifiable.getOperation();
    }
}

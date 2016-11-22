package io.github.vdubois.listener;

import io.github.vdubois.model.User;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

/**
 * Created by vdubois on 22/11/16.
 */
@Component
public class ReadUserListener implements ItemReadListener<User> {

    private static final Log LOGGER = LogFactory.getLog(ReadUserListener.class);

    /**
     * Called before {@link org.springframework.batch.item.ItemReader#read()}
     */
    @Override
    public void beforeRead() {

    }

    /**
     * Called after {@link org.springframework.batch.item.ItemReader#read()}
     *
     * @param item returned from read()
     */
    @Override
    public void afterRead(User item) {
        LOGGER.info(item);
    }

    /**
     * Called if an error occurs while trying to read.
     *
     * @param ex thrown from {@link org.springframework.batch.item.ItemWriter}
     */
    @Override
    public void onReadError(Exception ex) {
        LOGGER.error("Error reading user with exception ".concat(ex.getMessage()));
    }
}
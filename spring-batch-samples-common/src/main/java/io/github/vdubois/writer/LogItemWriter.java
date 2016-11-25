package io.github.vdubois.writer;

import lombok.extern.java.Log;
import org.springframework.batch.item.ItemWriter;

import java.util.List;


/**
 * Created by vdubois on 25/11/16.
 */
@Log
public class LogItemWriter implements ItemWriter {

    @Override
    public void write(List items) throws Exception {
        items.forEach(item -> log.info(item.toString()));
    }
}

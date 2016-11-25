package io.github.vdubois.aggregator;

import io.github.vdubois.model.Resource;
import org.springframework.batch.item.file.transform.LineAggregator;

import java.util.Map;

/**
 * Created by vdubois on 25/11/16.
 */
public class ResourceLineAggregator implements LineAggregator<Resource> {

    private Map<Class<LineAggregator<Resource>>, LineAggregator<Object>> aggregators;

    /**
     * Create a string from the value provided.
     *
     * @param item values to be converted
     * @return string
     */
    @Override
    public String aggregate(Resource item) {
        return aggregators.get(item.getClass()).aggregate(item);
    }

    /**
     * Sets aggregators.
     *
     * @param aggregators the aggregators
     */
    public void setAggregators(Map<Class<LineAggregator<Resource>>, LineAggregator<Object>> aggregators) {
        this.aggregators = aggregators;
    }
}

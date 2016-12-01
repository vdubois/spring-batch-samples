package io.github.vdubois.processor;

import org.dozer.Mapper;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by vdubois on 30/11/16.
 */
public class DozerMapperProcessor implements ItemProcessor<Object, Object> {

    private Mapper mapper;

    private Class toClass;

    private String mapId;

    @Override
    public Object process(Object item) throws Exception {
        Object mappedObject;
        if (mapId != null) {
            mappedObject = mapper.map(item, toClass, mapId);
        } else {
            mappedObject = mapper.map(item, toClass);
        }
        return mappedObject;
    }

    public void setMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public void setToClass(Class toClass) {
        this.toClass = toClass;
    }

    public void setMapId(String mapId) {
        this.mapId = mapId;
    }
}

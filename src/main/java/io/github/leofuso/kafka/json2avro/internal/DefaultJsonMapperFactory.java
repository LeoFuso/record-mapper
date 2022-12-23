package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.DatumFactory;
import io.github.leofuso.kafka.json2avro.JsonMapper;
import io.github.leofuso.kafka.json2avro.JsonMapperFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultJsonMapperFactory implements JsonMapperFactory {

    @Override
    public JsonMapper produce(final Conversion<?>... additional) {
        final DatumFactory factory = new DefaultDatumFactory(additional);
        final ObjectMapper mapper = ObjectMapperFactory.getInstance();
        return new DefaultJsonMapper(factory, mapper);
    }
}

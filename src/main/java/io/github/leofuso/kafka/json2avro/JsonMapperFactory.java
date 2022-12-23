package io.github.leofuso.kafka.json2avro;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.internal.DefaultJsonMapperFactory;
import io.github.leofuso.kafka.json2avro.internal.InstrumentedJsonMapperFactory;

public interface JsonMapperFactory {

    enum Strategy {
        RELAXED,
        DEFAULT
    }

    static JsonMapperFactory get() {
        return new InstrumentedJsonMapperFactory();
    }

    static JsonMapperFactory get(Strategy strategy) {
        return switch (strategy) {
            case DEFAULT -> new DefaultJsonMapperFactory();
            case RELAXED -> new InstrumentedJsonMapperFactory();
        };
    }

    JsonMapper produce(Conversion<?>... additional);

}

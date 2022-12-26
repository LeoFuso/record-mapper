package io.github.leofuso.kafka.json2avro.internal;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.RecordMapper;
import io.github.leofuso.kafka.json2avro.RecordMapperFactory;

import com.fasterxml.jackson.databind.json.JsonMapper;

public class DefaultRecordMapperFactory implements RecordMapperFactory {

    @Override
    public RecordMapper produce(final Conversion<?>... additional) {
        final JsonMapper mapper = JsonMapperFactory.getInstance();
        final DefaultRecordReaderWriterFactory factory = new DefaultRecordReaderWriterFactory(additional);
        return new DefaultRecordMapper(mapper, factory, factory);
    }
}

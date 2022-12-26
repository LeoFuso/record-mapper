package io.github.leofuso.kafka.json2avro.internal;

import io.github.leofuso.kafka.json2avro.RecordMapperFactory;

import org.apache.avro.Conversion;

import io.github.leofuso.kafka.json2avro.RecordMapper;

import com.fasterxml.jackson.databind.json.JsonMapper;

public class EnhancedRecordMapperFactory implements RecordMapperFactory {

    @Override
    public RecordMapper produce(Conversion<?>... additional) {
        final JsonMapper mapper = JsonMapperFactory.getInstance();
        final EnhancedRecordReaderWriterFactory factory = new EnhancedRecordReaderWriterFactory(additional);
        return new DefaultRecordMapper(mapper, factory, factory);
    }
}

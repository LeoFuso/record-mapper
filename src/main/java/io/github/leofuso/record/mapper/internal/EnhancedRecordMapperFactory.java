package io.github.leofuso.record.mapper.internal;

import io.github.leofuso.record.mapper.RecordMapperFactory;

import org.apache.avro.Conversion;

import io.github.leofuso.record.mapper.RecordMapper;

import com.fasterxml.jackson.databind.json.JsonMapper;

public class EnhancedRecordMapperFactory implements RecordMapperFactory {

    @Override
    public RecordMapper produce(Conversion<?>... additional) {
        final JsonMapper mapper = JsonMapperFactory.getInstance();
        final EnhancedRecordReaderWriterFactory factory = new EnhancedRecordReaderWriterFactory(additional);
        return new DefaultRecordMapper(mapper, factory, factory);
    }
}

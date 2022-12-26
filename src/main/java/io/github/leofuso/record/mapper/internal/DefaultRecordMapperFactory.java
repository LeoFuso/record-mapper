package io.github.leofuso.record.mapper.internal;

import io.github.leofuso.record.mapper.RecordMapperFactory;

import org.apache.avro.Conversion;

import io.github.leofuso.record.mapper.RecordMapper;

import com.fasterxml.jackson.databind.json.JsonMapper;

public class DefaultRecordMapperFactory implements RecordMapperFactory {

    @Override
    public RecordMapper produce(final Conversion<?>... additional) {
        final JsonMapper mapper = JsonMapperFactory.getInstance();
        final DefaultRecordReaderWriterFactory factory = new DefaultRecordReaderWriterFactory(additional);
        return new DefaultRecordMapper(mapper, factory, factory);
    }
}

package io.github.leofuso.kafka.json2avro.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.RelaxedGenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

import io.github.leofuso.kafka.json2avro.JsonMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class DefaultJsonMapper implements JsonMapper {

    private final GenericData data;
    private final ObjectMapper mapper;

    public DefaultJsonMapper(final GenericData data, final ObjectMapper mapper) {
        this.data = data;
        this.mapper = mapper;
    }

    @Override
    public GenericData.Record asGenericDataRecord(final InputStream valueStream, final Schema schema) {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            final ObjectReader reader = mapper.reader();
            final JsonNode node = reader.readTree(valueStream);

            mapper.writeValue(out, node);
            final byte[] nodeBytes = out.toByteArray();

            final ByteArrayInputStream datumReaderInput = new ByteArrayInputStream(nodeBytes);
            final JsonDecoder decoder = DecoderFactory.get()
                    .jsonDecoder(schema, datumReaderInput);

            final DatumReader<Object> datumReader = new RelaxedGenericDatumReader<>(schema, schema, data);
            final Object object = datumReader.read(null, decoder);
            return (GenericData.Record) object;

        } catch (IOException e) {
            throw new AvroMappingException("Unable to parse value.", e);
        }
    }

    @Override
    public <T extends GenericRecord> T asRecord(final InputStream valueStream, final Schema schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends GenericRecord> JsonNode asJsonNode(final T record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends GenericRecord> void serialize(final OutputStream outputStream, final T record) {
        throw new UnsupportedOperationException();
    }
}

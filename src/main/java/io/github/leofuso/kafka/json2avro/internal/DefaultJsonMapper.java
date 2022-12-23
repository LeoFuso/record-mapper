package io.github.leofuso.kafka.json2avro.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.RelaxedGenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

import io.github.leofuso.kafka.json2avro.JsonMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public class DefaultJsonMapper implements JsonMapper {

    private final GenericData data;
    private final ObjectMapper mapper;

    public DefaultJsonMapper(final GenericData data, final ObjectMapper mapper) {
        this.data = Objects.requireNonNull(data, "GenericData [data] is required.");
        this.mapper = Objects.requireNonNull(mapper, "ObjectMapper [mapper] is required.");
    }

    @Override
    public GenericData.Record asGenericDataRecord(final InputStream valueStream, final Schema schema) {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            final ObjectReader reader = mapper.reader();
            final JsonNode sourceNode = reader.readTree(valueStream);

            final JsonNode compatibleNode = toCompatibleNode(sourceNode, schema);
            mapper.writeValue(out, compatibleNode);
            final byte[] nodeBytes = out.toByteArray();

            final ByteArrayInputStream datumReaderInput = new ByteArrayInputStream(nodeBytes);
            final JsonDecoder decoder = DecoderFactory.get()
                    .jsonDecoder(schema, datumReaderInput);

            final DatumReader<Object> datumReader = new RelaxedGenericDatumReader<>(schema, schema, data);
            final Object object = datumReader.read(null, decoder);
            return (GenericData.Record) object;

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse value.", e);
        }
    }

    private JsonNode toCompatibleNode(final JsonNode sourceNode, final Schema schema) {
        final Schema.Type schemaType = schema.getType();
        final boolean isNotRecord = schemaType != Schema.Type.RECORD;
        if(isNotRecord) {
            return sourceNode.deepCopy();
        }
        final ObjectNode targetNode = mapper.createObjectNode();
        for (final Schema.Field field : schema.getFields()) {

            final String name = field.name();
            final String pathExpression = "/%s".formatted(name);
            final JsonNode childSourceNode = sourceNode.at(pathExpression);

            final JsonNodeType childSourceNodeType = childSourceNode.getNodeType();
            final JsonNode childTargetNode = switch (childSourceNodeType) {
                case OBJECT, ARRAY -> {
                    final Schema fieldSchema = field.schema();
                    yield toCompatibleNode(childSourceNode, fieldSchema);
                }
                default -> childSourceNode;
            };
            targetNode.set(name, childTargetNode);
        }
        return targetNode;
    }

    @Override
    public <T extends GenericRecord> T asRecord(final InputStream valueStream, final Schema schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends GenericRecord> JsonNode asJsonNode(final T record) {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
            serialize(outputStream, record);
            return mapper.readTree(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    public <T extends GenericRecord> void serialize(final OutputStream outputStream, final T record) {
        try {
            final Schema schema = record.getSchema();
            final NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(schema, outputStream);

            final DatumWriter<GenericRecord> writer =
                    record instanceof SpecificRecord ? new SpecificDatumWriter<>(schema) : new GenericDatumWriter<>(schema, data);

            writer.write(record, jsonEncoder);
            jsonEncoder.flush();

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }
}

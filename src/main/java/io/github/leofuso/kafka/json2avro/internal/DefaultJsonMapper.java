package io.github.leofuso.kafka.json2avro.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.DatumFactory;
import io.github.leofuso.kafka.json2avro.JsonMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultJsonMapper implements JsonMapper {

    private final DatumFactory datumFactory;
    private final ObjectMapper mapper;

    public DefaultJsonMapper(final DatumFactory datumFactory, final ObjectMapper mapper) {
        this.datumFactory = Objects.requireNonNull(datumFactory, "DatumFactory [datumFactory] is required.");
        this.mapper = Objects.requireNonNull(mapper, "ObjectMapper [mapper] is required.");
    }

    @Override
    public GenericData.Record asGenericDataRecord(final String json, final Schema schema) {
        try {

            final ObjectReader reader = mapper.reader();
            final JsonNode sourceNode = reader.readTree(json);

            final JsonNode compatibleNode = toCompatibleNode(sourceNode, schema);
            final String datumInput = mapper.writeValueAsString(compatibleNode);

            final DecoderFactory decoderFactory = DecoderFactory.get();
            final JsonDecoder decoder = decoderFactory.jsonDecoder(schema, datumInput);

            final DatumReader<GenericData.Record> datumReader = datumFactory.createReader(schema);
            return datumReader.read(null, decoder);

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse value.", e);
        }
    }

    private JsonNode toCompatibleNode(final JsonNode sourceNode, final Schema schema) {
        final Schema.Type schemaType = schema.getType();
        final boolean isNotRecord = schemaType != Schema.Type.RECORD;
        if (isNotRecord) {
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
    public <T extends SpecificRecord> T asRecord(final String json, Class<T> type) {

        final SpecificData specificData = SpecificData.getForClass(type);
        final Schema schema = specificData.getSchema(type);

        try (
                final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialize(json, schema));
                final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)
        ) {
            final DatumReader<T> reader = datumFactory.createReader(type);
            return reader.read(null, SpecificData.getDecoder(objectInputStream));
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    private byte[] serialize(final String json, final Schema schema) {
        try (
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)
        ) {

            final BinaryEncoder encoder = SpecificData.getEncoder(objectOutputStream);

            final GenericData.Record genericRecord = asGenericDataRecord(json, schema);
            final DatumWriter<GenericData.Record> writer = datumFactory.createWriter(schema);
            writer.write(genericRecord, encoder);
            encoder.flush();

            return outputStream.toByteArray();
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> JsonNode asJsonNode(final T record) {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final Schema schema = record.getSchema();
            final SpecificDatumWriter<T> writer = datumFactory.createWriter((Class<T>) record.getClass());

            final NoWrappingJsonEncoder encoder = new NoWrappingJsonEncoder(schema, outputStream);
            writer.write(record, encoder);

            encoder.flush();
            return mapper.readTree(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    public JsonNode asJsonNode(final GenericData.Record record) {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final Schema schema = record.getSchema();
            final DatumWriter<GenericData.Record> writer = datumFactory.createWriter(schema);

            final NoWrappingJsonEncoder encoder = new NoWrappingJsonEncoder(schema, outputStream);
            writer.write(record, encoder);

            encoder.flush();
            return mapper.readTree(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }
}

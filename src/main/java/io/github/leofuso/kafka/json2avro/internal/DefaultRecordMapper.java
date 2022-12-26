package io.github.leofuso.kafka.json2avro.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import io.github.leofuso.kafka.json2avro.RecordMapper;
import io.github.leofuso.kafka.json2avro.RecordReaderFactory;
import io.github.leofuso.kafka.json2avro.RecordWriterFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultRecordMapper implements RecordMapper {

    private final JsonMapper jsonMapper;

    private final RecordReaderFactory readerFactory;
    private final RecordWriterFactory writerFactory;

    public DefaultRecordMapper(JsonMapper mapper, RecordWriterFactory writerFactory, RecordReaderFactory readerFactory) {
        this.jsonMapper = Objects.requireNonNull(mapper, JsonMapper.class.getSimpleName() + " [mapper] is required.");
        this.writerFactory = Objects.requireNonNull(
                writerFactory,
                RecordWriterFactory.class.getSimpleName() + " [writerFactory] is required."
        );
        this.readerFactory = Objects.requireNonNull(
                readerFactory,
                RecordReaderFactory.class.getSimpleName() + " [readerFactory] is required."
        );
    }

    @Override
    public ByteBuffer serialize(final String json, final Schema schema) {
        Objects.requireNonNull(json, String.class.getSimpleName() + " [json] is required.");
        Objects.requireNonNull(schema, Schema.class.getSimpleName() + " [schema] is required.");

        final GenericData.Record record = asGenericDataRecord(json, schema);

        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final EncoderFactory encoderFactory = EncoderFactory.get();
            final BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);

            final DatumWriter<GenericData.Record> writer = writerFactory.produceWriter(schema);
            writer.write(record, encoder);
            encoder.flush();

            final byte[] byteArray = outputStream.toByteArray();
            return ByteBuffer.wrap(byteArray);

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    public GenericData.Record asGenericDataRecord(final String json, final Schema schema) {
        Objects.requireNonNull(json, String.class.getSimpleName() + " [json] is required.");
        Objects.requireNonNull(schema, Schema.class.getSimpleName() + " [schema] is required.");

        try {

            final ObjectReader reader = jsonMapper.reader();
            final JsonNode sourceNode = reader.readTree(json);

            final JsonNode compatibleNode = toCompatibleNode(sourceNode, schema);
            final String datumInput = jsonMapper.writeValueAsString(compatibleNode);

            final DecoderFactory decoderFactory = DecoderFactory.get();
            final JsonDecoder decoder = decoderFactory.jsonDecoder(schema, datumInput);

            final DatumReader<GenericData.Record> datumReader = readerFactory.produceReader(schema);
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
        final ObjectNode targetNode = jsonMapper.createObjectNode();
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
        Objects.requireNonNull(json, String.class.getSimpleName() + " [json] is required.");
        Objects.requireNonNull(type, Class.class.getSimpleName() + " [type] is required.");

        try {

            final SpecificData specificData = SpecificData.getForClass(type);
            final Schema schema = specificData.getSchema(type);

            final DecoderFactory decoderFactory = DecoderFactory.get();
            final ByteBuffer buffer = serialize(json, schema);
            final byte[] array = buffer.array();
            final BinaryDecoder decoder = decoderFactory.binaryDecoder(array, 0, array.length, null);

            final DatumReader<T> reader = readerFactory.produceReader(type);
            return reader.read(null, decoder);

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> JsonNode asJsonNode(final T record) {
        Objects.requireNonNull(record, " T [record] is required.");

        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final Schema schema = record.getSchema();
            final SpecificDatumWriter<T> writer = writerFactory.produceWriter((Class<T>) record.getClass());

            final NoWrappingJsonEncoder encoder = new NoWrappingJsonEncoder(schema, outputStream);
            writer.write(record, encoder);

            encoder.flush();
            return jsonMapper.readTree(outputStream.toByteArray());

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }

    @Override
    public JsonNode asJsonNode(final GenericData.Record record) {
        Objects.requireNonNull(record, GenericData.Record.class.getSimpleName() + " [record] is required.");

        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final Schema schema = record.getSchema();
            final DatumWriter<GenericData.Record> writer = writerFactory.produceWriter(schema);

            final NoWrappingJsonEncoder encoder = new NoWrappingJsonEncoder(schema, outputStream);
            writer.write(record, encoder);

            encoder.flush();
            return jsonMapper.readTree(outputStream.toByteArray());

        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse to JsonNode.", e);
        }
    }
}

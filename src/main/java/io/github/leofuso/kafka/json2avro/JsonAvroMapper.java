package io.github.leofuso.kafka.json2avro;

import java.io.*;
import java.nio.charset.*;

import org.apache.avro.*;
import org.apache.avro.data.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;

import io.confluent.kafka.schemaregistry.*;
import io.confluent.kafka.schemaregistry.avro.*;
import io.confluent.kafka.schemaregistry.utils.*;

import com.fasterxml.jackson.databind.*;

import org.apache.avro.specific.*;

public class JsonAvroMapper {

    private static final ObjectMapper mapper;
    private static final DecoderFactory decoderFactory;

    private static final GenericData DATA_INSTANCE;

    static {

        mapper = JacksonMapper.INSTANCE;

        decoderFactory = DecoderFactory.get();

        DATA_INSTANCE = new GenericData();
        DATA_INSTANCE.addLogicalTypeConversion(new Conversions.DecimalConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new Conversions.UUIDConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.DateConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        DATA_INSTANCE.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    }


    public <T extends GenericRecord> T toRecord(final byte[] data, final ParsedSchema schema) {
        try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            return toRecord(inputStream, schema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends GenericRecord> T toRecord(final InputStream inputStream, final ParsedSchema schema) {
        try {
            final ObjectReader reader = mapper.reader();
            final JsonNode jsonNode = reader.readTree(inputStream);

            @SuppressWarnings("unchecked")
            final T record = (T) AvroSchemaUtils.toObject(jsonNode, (AvroSchema) schema);
            return record;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public GenericData.Record toGenericDataRecord(final byte[] data, final Schema schema) {

        try (
                final ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                final ByteArrayOutputStream out = new ByteArrayOutputStream()
        ) {

            final ObjectReader reader = mapper.reader();
            final JsonNode node = reader.readTree(inputStream);

            mapper.writeValue(out, node);
            final byte[] nodeBytes = out.toByteArray();

            final ByteArrayInputStream datumReaderInput = new ByteArrayInputStream(nodeBytes);
            final JsonDecoder decoder = decoderFactory.jsonDecoder(schema, datumReaderInput);

            final DatumReader<Object> datumReader = new GenericDatumReader<>(schema, schema, DATA_INSTANCE);
            final Object object = datumReader.read(null, decoder);

            return (GenericData.Record) object;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public GenericData.Record toGenericDataRecord(final byte[] data, final ParsedSchema schema) {
        return toRecord(data, schema);
    }

    public GenericData.Record toGenericDataRecord(final InputStream inputStream, final ParsedSchema schema) {
        return toRecord(inputStream, schema);
    }

    <T extends GenericRecord> byte[] toSerializedJson(T record) {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){

            final Schema schema = record.getSchema();
            final NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(schema, outputStream);

            final DatumWriter<GenericRecord> writer =
                    record instanceof SpecificRecord ? new SpecificDatumWriter<>(schema) : new GenericDatumWriter<>(schema, DATA_INSTANCE);

            writer.write(record, jsonEncoder);
            jsonEncoder.flush();

            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert to JSON.", e);
        }
    }

    <T extends GenericRecord> String toJson(T record) {
        final byte[] bytes = toSerializedJson(record);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    <T extends GenericRecord> JsonNode toJsonNode(T record) {
        try {
            final byte[] bytes = toSerializedJson(record);
            return mapper.readTree(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

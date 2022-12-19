package io.github.leofuso.kafka.json2avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A JsonMapper can produce {@link GenericRecord records} {@link Schema Schema-compatible} from a relaxed JSON-compatible byte array, and
 * can produce a JSON-compatible byte array from an avro compliant {@link GenericRecord record}.
 * <p>
 * Please note that this utility class was designed to be used in a non-production environment. Its main purpose is helping throughout the
 * development cycle.
 * <p>
 * There are no performance guarantees.
 */
public interface JsonMapper {

    default GenericData.Record asGenericDataRecord(byte[] value, Schema schema) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value)) {
            return asGenericDataRecord(byteArrayInputStream, schema);
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse value.", e);
        }
    }

    default <T extends GenericRecord> T asRecord(byte[] value, Schema schema) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value)) {
            return asRecord(byteArrayInputStream, schema);
        } catch (final IOException e) {
            throw new AvroMappingException("Unable to parse value.", e);
        }
    }

    GenericData.Record asGenericDataRecord(InputStream valueStream, Schema schema);

    <T extends GenericRecord> T asRecord(InputStream valueStream, Schema schema);

    <T extends GenericRecord> JsonNode asJsonNode(T record);

    <T extends GenericRecord> void serialize(OutputStream outputStream, T record);

    class AvroMappingException extends AvroRuntimeException {

        /**
         * Constructs a new runtime exception with the specified detail message and cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in this runtime exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval by the {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method).  (A {@code null} value is
         *                permitted, and indicates that the cause is nonexistent or unknown.)
         */
        public AvroMappingException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}

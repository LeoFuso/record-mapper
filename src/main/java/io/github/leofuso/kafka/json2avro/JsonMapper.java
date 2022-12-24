package io.github.leofuso.kafka.json2avro;

import java.io.OutputStream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

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

    GenericData.Record asGenericDataRecord(String json, Schema schema);

    <T extends SpecificRecord> T asRecord(String json, Class<T> type);

    JsonNode asJsonNode(GenericData.Record record);

    <T extends SpecificRecord> JsonNode asJsonNode(T record);

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

package io.github.leofuso.kafka.json2avro;

import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * RecordMapper provides functionality for reading and writing JSON {@link Schema Schema-compatible}, either to and from
 * {@link GenericRecord records}, or to and from a general-purpose JSON Tree Model ({@link JsonNode}).
 *
 * <p>
 * Please note that this utility class was designed to be used in a non-production environment. Its main purpose is helping throughout the
 * development cycle.
 * <p>
 * There are no performance guarantees or direct Cache utilizations.
 */
public interface RecordMapper {

    /**
     * Writes a {@code JSON} into a serialized {@link GenericRecord Record} wrapped in a {@link ByteBuffer ByteBuffer}.
     *
     * @return the resulting {@link ByteBuffer}.
     */
    ByteBuffer serialize(String json, Schema schema);

    /**
     * Writes a {@code JSON} into a {@link Schema Schema-compatible} {@link GenericData.Record}.
     *
     * @return the resulting {@link GenericData.Record}.
     */
    GenericData.Record asGenericDataRecord(String json, Schema schema);

    /**
     * Writes a {@code JSON} into a {@link SpecificRecord type}.
     *
     * @return the resulting {@link SpecificRecord}.
     */
    <T extends SpecificRecord> T asRecord(String json, Class<T> type);

    /**
     * Writes a {@link GenericData.Record} into a {@link JsonNode}.
     *
     * @return the resulting {@link JsonNode}.
     */
    JsonNode asJsonNode(GenericData.Record record);

    /**
     * Writes a {@link SpecificRecord} into a {@link JsonNode}.
     *
     * @return the resulting {@link JsonNode}.
     */
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

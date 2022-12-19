package org.apache.avro.generic;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class RelaxedGenericDatumReader<D> extends GenericDatumReader<D> {

    public RelaxedGenericDatumReader() {}

    /**
     * Construct where the writer's and reader's schemas are the same.
     */
    public RelaxedGenericDatumReader(final Schema schema) {
        super(schema);
    }

    /**
     * Construct given writer's and reader's schema.
     */
    public RelaxedGenericDatumReader(final Schema writer, final Schema reader) {
        super(writer, reader);
    }

    public RelaxedGenericDatumReader(final Schema writer, final Schema reader, final GenericData data) {
        super(writer, reader, data);
    }

    protected RelaxedGenericDatumReader(final GenericData data) {
        super(data);
    }

    /**
     * Called to read byte arrays. Subclasses may override to use a different byte
     * array representation. By default, this calls
     * {@link Decoder#readBytes(ByteBuffer)}.
     *
     */
    @Override
    protected Object readBytes(final Object old, final Schema s, final Decoder in) throws IOException {
        return super.readBytes(old, s, in);
    }

    /**
     * Called to read byte arrays. Subclasses may override to use a different byte
     * array representation. By default, this calls
     * {@link Decoder#readBytes(ByteBuffer)}.
     *
     */
    @Override
    protected Object readBytes(final Object old, final Decoder in) throws IOException {
        try {
            final Class<? extends Decoder> decoderClass = in.getClass();
            final Method relaxedReadBytesMethod = decoderClass.getMethod("relaxedReadBytes", ByteBuffer.class);
            return relaxedReadBytesMethod.invoke(in, old instanceof ByteBuffer byteBuffer ? byteBuffer : null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    /**
     * Convert an underlying representation of a logical type (such as a ByteBuffer)
     * to a higher level object (such as a BigDecimal).
     */
    @Override
    protected Object convert(final Object datum, final Schema schema, final LogicalType type, final Conversion<?> conversion) {
        if (datum == null) {
            return null;
        }

        if (schema == null || type == null || conversion == null) {
            throw new IllegalArgumentException("Parameters cannot be null! Parameter values:"
                                                       + Arrays.deepToString(new Object[] { datum, schema, type, conversion }));
        }

        try {
            return switch (schema.getType()) {
                case RECORD -> conversion.fromRecord((IndexedRecord) datum, schema, type);
                case ENUM -> conversion.fromEnumSymbol((GenericEnumSymbol<?>) datum, schema, type);
                case ARRAY -> conversion.fromArray((Collection<?>) datum, schema, type);
                case MAP -> conversion.fromMap((Map<?, ?>) datum, schema, type);
                case FIXED -> conversion.fromFixed((GenericFixed) datum, schema, type);
                case STRING -> conversion.fromCharSequence((CharSequence) datum, schema, type);
                case BYTES -> {
                    if (datum instanceof ByteBuffer byteBuffer) {
                        yield conversion.fromBytes(byteBuffer, schema, type);
                    }
                    if (datum instanceof Double doubleValue) {
                        yield conversion.fromDouble(doubleValue, schema, type);
                    }
                     yield datum;
                }
                case INT -> conversion.fromInt((Integer) datum, schema, type);
                case LONG -> conversion.fromLong((Long) datum, schema, type);
                case FLOAT -> conversion.fromFloat((Float) datum, schema, type);
                case DOUBLE -> conversion.fromDouble((Double) datum, schema, type);
                case BOOLEAN -> conversion.fromBoolean((Boolean) datum, schema, type);
                default -> datum;
            };
        } catch (ClassCastException e) {
            throw new AvroRuntimeException(
                    "Cannot convert " + datum + ":" + datum.getClass().getSimpleName() + ": expected generic type", e);
        }
    }
}

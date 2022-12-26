package org.apache.avro.generic;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.github.leofuso.kafka.json2avro.exception.Throwables;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_BYTES_REWRITE;
import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_INT_REWRITE;
import static io.github.leofuso.kafka.json2avro.instrument.InterceptorDispatcher.READ_LONG_REWRITE;

/**
 *  {@link DatumReader} for generic Java objects.
 *  <p>
 *  Enhanced implementation accepts relaxed values, such as BigDecimal('9.25') instead of the canonical string only version.
 */
public class EnhancedGenericDatumReader<D> extends GenericDatumReader<D> {

    public EnhancedGenericDatumReader() {}

    /**
     * Construct where the writer's and reader's schemas are the same.
     */
    public EnhancedGenericDatumReader(final Schema schema) {
        super(schema);
    }

    /**
     * Construct given writer's and reader's schema.
     */
    public EnhancedGenericDatumReader(final Schema writer, final Schema reader) {
        super(writer, reader);
    }

    public EnhancedGenericDatumReader(final Schema writer, final Schema reader, final GenericData data) {
        super(writer, reader, data);
    }

    protected EnhancedGenericDatumReader(final GenericData data) {
        super(data);
    }


    /**
     * Relaxed re-implementation of {@link GenericDatumReader#readWithoutConversion(Object, Schema, ResolvingDecoder)} aimed to redirect
     * specific parser functions to overriden ones, e.g., Expecting a Long field, but found a CharSequence value instead.
     */
    protected Object readWithoutConversion(final Object old, final Schema expected, final ResolvingDecoder in) throws IOException {
        final Schema.Type expectedType = expected.getType();
        return switch (expectedType) {
            case RECORD -> readRecord(old, expected, in);
            case ENUM -> readEnum(expected, in);
            case ARRAY -> readArray(old, expected, in);
            case MAP -> readMap(old, expected, in);
            case UNION -> {
                final int index = in.readIndex();
                final List<Schema> types = expected.getTypes();
                yield read(old, types.get(index), in);
            }
            case FIXED -> readFixed(old, expected, in);
            case STRING -> readString(old, expected, in);
            case BYTES -> invokeEnhanced(READ_BYTES_REWRITE, in);
            case INT -> invokeEnhanced(READ_INT_REWRITE, in);
            case LONG -> invokeEnhanced(READ_LONG_REWRITE, in);
            case FLOAT -> in.readFloat();
            case DOUBLE -> in.readDouble();
            case BOOLEAN -> in.readBoolean();
            case NULL -> {
                in.readNull();
                yield null;
            }
        };
    }

    private Object invokeEnhanced(final String name, final ResolvingDecoder in, final Object ... args) {
        try {
            final Class<? extends Decoder> decoderClass = in.getClass();
            final Method method = decoderClass.getMethod(name);
            return method.invoke(in, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            Throwables.handleReflectionException(e);
            return null; /* Unreachable code */
        }
    }

    /**
     * Convert an underlying representation of a logical type (such as a ByteBuffer) to a higher level object (such as a BigDecimal).
     */
    @Override
    protected Object convert(final Object datum, final Schema schema, final LogicalType type, final Conversion<?> conversion) {
        if (datum == null) {
            return null;
        }

        if (schema == null || type == null || conversion == null) {
            final String parameters = Arrays.deepToString(new Object[] { datum, schema, type, conversion });
            throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" + parameters);
        }

        try {
            final Schema.Type expectedType = schema.getType();
            return switch (expectedType) {
                case RECORD -> conversion.fromRecord((IndexedRecord) datum, schema, type);
                case ENUM -> conversion.fromEnumSymbol((GenericEnumSymbol<?>) datum, schema, type);
                case ARRAY -> conversion.fromArray((Collection<?>) datum, schema, type);
                case MAP -> conversion.fromMap((Map<?, ?>) datum, schema, type);
                case FIXED -> conversion.fromFixed((GenericFixed) datum, schema, type);
                case STRING -> conversion.fromCharSequence((CharSequence) datum, schema, type);
                case BYTES -> {
                    if (datum instanceof ByteBuffer value) {
                        yield conversion.fromBytes(value, schema, type);
                    }
                    if (datum instanceof Double value) {
                        yield conversion.fromDouble(value, schema, type);
                    }
                    if (datum instanceof Integer value) {
                        yield conversion.fromInt(value, schema, type);
                    }
                    yield datum;
                }
                case INT -> {
                    if (datum instanceof Integer value) {
                        yield conversion.fromInt(value, schema, type);
                    }
                    if (datum instanceof CharSequence value) {
                        yield conversion.fromCharSequence(value, schema, type);
                    }
                    yield datum;
                }
                case LONG -> {
                    if (datum instanceof Long value) {
                        yield conversion.fromLong(value, schema, type);
                    }
                    if (datum instanceof CharSequence value) {
                        yield conversion.fromCharSequence(value, schema, type);
                    }
                    yield datum;
                }
                case FLOAT -> conversion.fromFloat((Float) datum, schema, type);
                case DOUBLE -> conversion.fromDouble((Double) datum, schema, type);
                case BOOLEAN -> conversion.fromBoolean((Boolean) datum, schema, type);
                default -> datum;
            };
        } catch (final ClassCastException e) {
            final Class<?> objectClass = datum.getClass();
            final String objectClassName = objectClass.getSimpleName();
            final String exceptionMessage = "Cannot convert %s:%s: expected generic type".formatted(objectClass, objectClassName);
            throw new AvroRuntimeException(exceptionMessage, e);
        }
    }
}

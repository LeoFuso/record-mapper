package org.apache.avro;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.record.mapper.exception.Throwables;

public class EnhancedDecimalConversion extends Conversions.DecimalConversion {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedDecimalConversion.class);

    @Override
    public BigDecimal fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {
        try {

            final byte[] bytes = new byte[value.remaining()];
            final ByteBuffer duplicated = value.duplicate();
            duplicated.get(bytes);

            final String possibleStringValue = new String(bytes, StandardCharsets.ISO_8859_1);
            final BigDecimal decimal = new BigDecimal(possibleStringValue);

            doValidate((LogicalTypes.Decimal) type, decimal);
            return decimal;

        } catch (final NumberFormatException ignored) {
            final boolean traceEnabled = logger.isTraceEnabled();
            if (traceEnabled) {
                logger.trace("ByteBuffer value does not parse to BigDecimal. Applying default DecimalConversion behavior.");
            }
            return super.fromBytes(value, schema, type);
        } catch (final InvocationTargetException e) {
            Throwables.handleReflectionException(e);
            return null; /* Unreacheable code */
        } catch (final Exception e) {
            final String errorMessage = "Conversion failure while converting from Byte[] %s to BigDecimal Logical type.".formatted(value);
            throw new AvroTypeException(errorMessage, e);
        }
    }

    @Override
    public BigDecimal fromDouble(final Double value, final Schema schema, final LogicalType type) {
        try {

            final BigDecimal decimal = BigDecimal.valueOf(value);
            doValidate((LogicalTypes.Decimal) type, decimal);
            return decimal;

        } catch (final InvocationTargetException e) {
            Throwables.handleReflectionException(e);
            return null; /* Unreacheable code */
        } catch (final Exception e) {
            final String errorMessage = "Conversion failure while converting from Double %s to BigDecimal Logical type.".formatted(value);
            throw new AvroTypeException(errorMessage, e);
        }
    }

    @Override
    public BigDecimal fromInt(final Integer value, final Schema schema, final LogicalType type) {
        try {

            final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) type;
            final int scale = decimalType.getScale();

            final BigDecimal decimal = BigDecimal.valueOf(value, scale);
            doValidate(decimalType, decimal);
            return decimal;

        } catch (final InvocationTargetException e) {
            Throwables.handleReflectionException(e);
            return null; /* Unreacheable code */
        } catch (final Exception e) {
            final String errorMessage = "Conversion failure while converting from Integer %s to BigDecimal Logical type.".formatted(value);
            throw new AvroTypeException(errorMessage, e);
        }
    }

    private void doValidate(final LogicalTypes.Decimal type, final BigDecimal value) throws Exception {
        final Method validate = Conversions.DecimalConversion.class.getDeclaredMethod(
                "validate",
                LogicalTypes.Decimal.class,
                BigDecimal.class
        );
        validate.setAccessible(true);
        validate.invoke(null, type, value);
    }
}

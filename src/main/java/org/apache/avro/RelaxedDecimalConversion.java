package org.apache.avro;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RelaxedDecimalConversion extends Conversions.DecimalConversion {

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

        } catch (final NumberFormatException ex) {
            return super.fromBytes(value, schema, type);
        } catch (final AvroTypeException validationEx) {
            throw validationEx;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BigDecimal fromDouble(final Double value, final Schema schema, final LogicalType type) {
        try {

            final BigDecimal decimal = BigDecimal.valueOf(value);
            doValidate((LogicalTypes.Decimal) type, decimal);
            return decimal;

        } catch (final AvroTypeException validationEx) {
            throw validationEx;
        } catch (final Exception e) {
            throw new AvroTypeException("BigDecimal value violates logical type.", e);
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

        } catch (final AvroTypeException validationEx) {
            throw validationEx;
        } catch (final Exception e) {
            throw new AvroTypeException("BigDecimal value violates logical type.", e);
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

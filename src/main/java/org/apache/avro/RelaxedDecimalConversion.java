package org.apache.avro;

import java.lang.reflect.Method;
import java.math.BigDecimal;

public class RelaxedDecimalConversion extends Conversions.DecimalConversion {

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

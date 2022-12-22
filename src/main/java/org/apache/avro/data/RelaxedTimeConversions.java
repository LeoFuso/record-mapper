package org.apache.avro.data;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.Instant;

public class RelaxedTimeConversions extends TimeConversions {

    public static final class TimestampMillisConversion extends TimeConversions.TimestampMillisConversion {
        @Override
        public Instant fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            return Instant.parse(value);
        }
    }

    public static final class TimestampMicrosConversion extends TimeConversions.TimestampMicrosConversion {
        @Override
        public Instant fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            return Instant.parse(value);
        }
    }

}

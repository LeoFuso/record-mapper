package org.apache.avro.data;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

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


    public static class TimeMillisConversion extends TimeConversions.TimeMillisConversion {

        @Override
        public LocalTime fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            return LocalTime.parse(value);
        }
    }


    public static class TimeMicrosConversion extends TimeConversions.TimeMicrosConversion {

        @Override
        public LocalTime fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            return LocalTime.parse(value);
        }
    }


    public static class DateConversion extends TimeConversions.DateConversion {

        @Override
        public LocalDate fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            return LocalDate.parse(value);
        }
    }

}

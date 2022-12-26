package io.github.leofuso.record.mapper;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.time.format.DateTimeParseException;

import io.github.leofuso.record.mapper.fixture.JsonParameterResolver;
import io.github.leofuso.record.mapper.fixture.SchemaParameterResolver;
import io.github.leofuso.record.mapper.fixture.annotation.JsonParameter;
import io.github.leofuso.record.mapper.fixture.annotation.SchemaParameter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Timestamp with microsecond precision LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class TimestampMicrosTest {

    private static RecordMapper mapper;

    @BeforeAll
    static void setUp() {
        final RecordMapperFactory mapperFactory = RecordMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given a long-typed timestamp value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to Instant timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .asInstanceOf(InstanceOfAssertFactories.type(Instant.class))
                .isEqualTo(Instant.parse("2022-12-18T04:51:55.565970Z"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid long-typed timestamp value,
                    When converting to GenericData.Record,
                    Then should fail the Instant amount field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71784(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.invalid.long.json") String json
    ) {

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(json, schema))
                .isInstanceOf(UndeclaredThrowableException.class)
                .hasRootCauseMessage(
                        "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - 9223372036854775807)\n" +
                                " at [Source: (String)\"{\"timestamp\":9223372036854775808}\"; line: 1, column: 33]"
                );
    }

    @Test
    @DisplayName(
            """
                    Given a standard string value (ISO-88601),
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to Instant timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.ISO-8601.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .asInstanceOf(InstanceOfAssertFactories.type(Instant.class))
                .isEqualTo(Instant.parse("2022-12-18T04:51:55.565970Z"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid standard string value (ISO-88601),
                    When converting to GenericData.Record,
                    Then should fail the Instant amount field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71785(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.invalid.ISO-8601.json") String json
    ) {

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(json, schema))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessage("Text 'Tue, 3 Jun 2008 11:05:30 GMT' could not be parsed at index 0");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed long-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to Instant timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.zeroed.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a null long-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to Instant timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "timestamp.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "timestamp/micros/timestamp.null.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .isNull();
    }
}

package io.github.leofuso.kafka.json2avro;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.leofuso.kafka.json2avro.fixture.JsonParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.SchemaParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.annotation.JsonParameter;
import io.github.leofuso.kafka.json2avro.fixture.annotation.SchemaParameter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Local Timestamp with millisecond precision LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class LocalTimestampMillisTest {

    private static JsonMapper mapper;

    @BeforeAll
    static void setUp() {
        final JsonMapperFactory mapperFactory = JsonMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given a long-typed timestamp value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDateTime local-timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.long.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalDateTime.class))
                .isEqualTo(LocalDateTime.parse("2022-12-18T04:51:55.565"));
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
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.invalid.long.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final String expectedError = "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - " +
                "9223372036854775807)\n" +
                " at [Source: (ByteArrayInputStream); line: 1, column: 33]";

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(UndeclaredThrowableException.class)
                .hasRootCauseMessage(expectedError);
    }

    @Test
    @DisplayName(
            """
                    Given a standard string value (ISO-88601),
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDateTime local-timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalDateTime.class))
                .isEqualTo(LocalDateTime.parse("2022-12-18T04:51:55.565"));
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
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.invalid.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessage("Text 'Tue, 3 Jun 2008 11:05:30' could not be parsed at index 0");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed long-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDateTime local-timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.zeroed.long.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

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
                    Then should apply the corresponding value to LocalDateTime local-timestamp field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "local.timestamp.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "local.timestamp/millis/timestamp.null.long.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("timestamp"))
                .isNull();
    }
}

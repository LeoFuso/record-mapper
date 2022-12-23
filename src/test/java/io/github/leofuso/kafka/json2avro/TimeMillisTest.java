package io.github.leofuso.kafka.json2avro;

import io.github.leofuso.kafka.json2avro.fixture.JsonParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.SchemaParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.annotation.JsonParameter;
import io.github.leofuso.kafka.json2avro.fixture.annotation.SchemaParameter;
import io.github.leofuso.kafka.json2avro.instrument.bytecode.ByteCodeRewriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Time with millisecond precision LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class TimeMillisTest {

    private static JsonMapper mapper;

    @BeforeAll
    static void setUp() {
        ByteCodeRewriter.rewrite();
        final JsonMapperFactory mapperFactory = JsonMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given an int-typed time value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalTime.class))
                .isEqualTo(LocalTime.parse("04:51:55.565"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid int-typed time value,
                    When converting to GenericData.Record,
                    Then should fail the LocalTime time field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71784(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.invalid.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final String expectedError = "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)\n" +
                " at [Source: (ByteArrayInputStream); line: 1, column: 19]";

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
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalTime.class))
                .isEqualTo(LocalTime.parse("04:51:55.565"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid standard string value (ISO-88601),
                    When converting to GenericData.Record,
                    Then should fail the LocalTime time field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71785(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.invalid.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessage("Text '12 PM' could not be parsed at index 2");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed int-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.zeroed.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a null int-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "time.millis.schema.avsc") Schema schema,
            @JsonParameter(location = "time/millis/time.null.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .isNull();
    }
}

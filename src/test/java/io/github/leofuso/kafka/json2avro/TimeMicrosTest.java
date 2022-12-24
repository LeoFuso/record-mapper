package io.github.leofuso.kafka.json2avro;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.LocalTime;
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

@DisplayName("Time with microsecond precision LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class TimeMicrosTest {

    private static JsonMapper mapper;

    @BeforeAll
    static void setUp() {
        final JsonMapperFactory mapperFactory = JsonMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given a long-typed time value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalTime.class))
                .isEqualTo(LocalTime.parse("04:51:55.565970"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid long-typed timestamp value,
                    When converting to GenericData.Record,
                    Then should fail the LocalTime time field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71784(
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.invalid.long.json") String json
    ) {

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(json, schema))
                .isInstanceOf(UndeclaredThrowableException.class)
                .hasRootCauseMessage(
                        "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - 9223372036854775807)\n" +
                                " at [Source: (String)\"{\"time\":9223372036854775808}\"; line: 1, column: 28]"
                );
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
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.ISO-8601.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalTime.class))
                .isEqualTo(LocalTime.parse("04:51:55.565970"));
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
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.invalid.ISO-8601.json") String json
    ) {

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(json, schema))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessage("Text '12 PM' could not be parsed at index 2");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed long-typed timestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.zeroed.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a null long-typed time value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalTime time field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "time.micros.schema.avsc") Schema schema,
            @JsonParameter(location = "time/micros/time.null.long.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("time"))
                .isNull();
    }
}

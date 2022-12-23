package io.github.leofuso.kafka.json2avro;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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

@DisplayName("Date LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class DateTest {

    private static JsonMapper mapper;

    @BeforeAll
    static void setUp() {
        final JsonMapperFactory mapperFactory = JsonMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given an int-typed date value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDate date field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("date"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalDate.class))
                .isEqualTo(LocalDate.parse("2008-06-03"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid int-typed date value,
                    When converting to GenericData.Record,
                    Then should fail the LocalDate date field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71784(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.invalid.int.json") String json
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
                    Then should apply the corresponding value to LocalDate date field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("date"))
                .asInstanceOf(InstanceOfAssertFactories.type(LocalDate.class))
                .isEqualTo(LocalDate.parse("2008-06-03"));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid standard string value (ISO-88601),
                    When converting to GenericData.Record,
                    Then should fail the LocalDate date field convertion
                    """
    )
    void b8aec7e506ce410bb646f517cf71785(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.invalid.ISO-8601.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessage("Text 'Tue, 3 Jun 2008' could not be parsed at index 0");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed int-typed datestamp value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDate date field
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.zeroed.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("date"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a null int-typed date value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to LocalDate date field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "date.schema.avsc") Schema schema,
            @JsonParameter(location = "date/date.null.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("date"))
                .isNull();
    }
}

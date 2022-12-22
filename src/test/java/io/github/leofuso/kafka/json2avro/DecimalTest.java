package io.github.leofuso.kafka.json2avro;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
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
import io.github.leofuso.kafka.json2avro.instrument.bytecode.ByteCodeRewriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Decimal LogicalType unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class DecimalTest {

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
                    Given a double-typed decimal value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.double.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(BigDecimal.valueOf(19565, 3));

    }

    @Test
    @DisplayName(
            """
                    Given an invalid double-typed decimal value,
                    When converting to GenericData.Record,
                    Then should fail the BigDecimal amount field validation
                    """
    )
    void b8aec7e506ce410bb646f517cf71784a(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.invalid.double.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(AvroTypeException.class)
                .hasMessage("Cannot encode decimal with scale 4 as scale 3 without rounding");
    }

    @Test
    @DisplayName(
            """
                    Given a stringified double-typed decimal value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784d(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.string.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(BigDecimal.valueOf(19565, 3));

    }

    @Test
    @DisplayName(
            """
                    Given an invalid stringified double-typed decimal value,
                    When converting to GenericData.Record,
                    Then should fail the BigDecimal amount field validation
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.invalid.string.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(AvroTypeException.class)
                .hasMessage("Cannot encode decimal with scale 4 as scale 3 without rounding");
    }

    @Test
    @DisplayName(
            """
                    Given a gibberish value,
                    When converting to GenericData.Record,
                    Then should an equal gibberish BigDecimal amount field be returned
                    """
    )
    void b8aec7e506ce410bb646f517cf717848(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.gibberish.string.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(new BigDecimal("1907608379701634626.408"));
    }

    @Test
    @DisplayName(
            """
                    Given a standard decimal value (ISO-8859-1),
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784e(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.ISO-8859-1.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(BigDecimal.valueOf(19565, 3));
    }

    @Test
    @DisplayName(
            """
                    Given an invalid standard decimal value (ISO-8859-1),
                    When converting to GenericData.Record,
                    Then should fail the BigDecimal amount field validation
                    """
    )
    void b8aec7e506ce410bb646f517cf717843(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.invalid.ISO-8859-1.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(AvroTypeException.class)
                .hasMessage("Cannot encode decimal with scale 4 as scale 3 without rounding");
    }

    @Test
    @DisplayName(
            """
                    Given an int-typed decimal value,
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.int.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(BigDecimal.valueOf(19565, 3));

    }

    @Test
    @DisplayName(
            """
                    Given an invalid int-typed decimal value,
                    When converting to GenericData.Record,
                    Then should fail the BigDecimal amount field validation
                    """
    )
    void b8aec7e506ce410bb646f517cf717847(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.invalid.string.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When then */
        assertThatThrownBy(() -> mapper.asGenericDataRecord(bytes, schema))
                .isInstanceOf(AvroTypeException.class)
                .hasMessage("Cannot encode decimal with scale 4 as scale 3 without rounding");
    }

    @Test
    @DisplayName(
            """
                    Given a zeroed double-typed decimal value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf717844(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.zeroed.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a null double-typed decimal value
                    When converted to GenericData.Record,
                    Then should apply the corresponding value to the BigDecimal amount field
                    """
    )
    void b8aec7e506ce410bb646f517cf717840(
            @SchemaParameter(location = "decimal.schema.json") Schema schema,
            @JsonParameter(location = "decimal/decimal.null.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .isNull();
    }
}

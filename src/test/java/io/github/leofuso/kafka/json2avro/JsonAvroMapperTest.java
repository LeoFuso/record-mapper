package io.github.leofuso.kafka.json2avro;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.leofuso.kafka.json2avro.fixture.JsonParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.SchemaParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.annotation.JsonParameter;
import io.github.leofuso.kafka.json2avro.fixture.annotation.SchemaParameter;
import io.github.leofuso.kafka.json2avro.instrument.bytecode.ByteCodeRewriter;
import io.github.leofuso.kafka.json2avro.internal.ObjectMapperFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JsonAvroMapper Unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
class JsonAvroMapperTest {

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
                    Given a friendly json template with a valid Schema,
                    When converted to GenericData.Record,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @SchemaParameter(location = "statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v2.template.json") String json
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThatRecord(schema, record);
    }

    @Test
    @DisplayName(
            """
                    Given a canonical-form json template with a valid Schema,
                    When converted to GenericData.Record,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf71784d(
            @SchemaParameter(location = "statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v1.template.json") String json
    ) throws JsonProcessingException {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
        assertThatRecord(schema, record);
    }

    @Test
    @DisplayName(
            """
                    Given a GenericData.Record,
                    When converted to JsonNode,
                    Then the values must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @SchemaParameter(location = "avro/statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v2.template.json") String json
    ) throws JsonProcessingException {

        /* Given */
        final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
        final JsonNode expectedJsonNode = objectMapper.readTree(json);

        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* When */
        final JsonNode actualJsonNode = mapper.asJsonNode(record);

        /* Then */
        assertThat(actualJsonNode)
                .isEqualTo(expectedJsonNode);
    }

    private static void assertThatRecord(final Schema schema, final GenericData.Record record) {
        assertThat(record)
                .isNotNull()
                .satisfies(input -> {

                    assertThat(input).extracting(i -> i.get("transaction"))
                            .isEqualTo(UUID.fromString("53a50a63-93ba-5ffd-b8cf-981a73947a7f"));

                    assertThat(input).extracting(i -> i.get("source"))
                            .asInstanceOf(InstanceOfAssertFactories.type(GenericData.Record.class))
                            .extracting(i -> i.get("namespace"))
                            .isEqualTo(UUID.fromString("74074428-7c4d-4922-b8fd-7c32e9ae499b"));

                    assertThat(input).extracting(i -> i.get("source"))
                            .asInstanceOf(InstanceOfAssertFactories.type(GenericData.Record.class))
                            .extracting(i -> i.get("domain"))
                            .isEqualTo(new Utf8("obs.demo"));

                    assertThat(input).extracting(i -> i.get("account"))
                            .isEqualTo(UUID.fromString("97175496-7cb2-468e-a812-015f7a44450f"));

                    assertThat(input).extracting(i -> i.get("operation"))
                            .isEqualTo(new GenericData.EnumSymbol(schema, "CREDIT"));

                    assertThat(input).extracting(i -> i.get("amount"))
                            .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                            .usingComparator(BigDecimal::compareTo)
                            .isEqualTo(BigDecimal.valueOf(19565, 3));

                    assertThat(input).extracting(i -> i.get("competence"))
                            .isEqualTo(Instant.parse("2022-12-18T04:51:55.565970Z"));

                    assertThat(input).extracting(i -> i.get("date"))
                            .isEqualTo(LocalDate.parse("2022-12-22"));

                    assertThat(input).extracting(i -> i.get("time"))
                            .isEqualTo(LocalTime.parse("20:23:59.059"));

                    final String baggageValue =
                            "f1a4cfc3-b1a2-459d-ab0c-d7ac3cac42d0, 126cefe4-aab4-4bb6-9876-e245c834b0ac, " +
                                    "4475ad68-5c74-40bb-ae82-c79dfab3f149";
                    assertThat(input).extracting(i -> i.get("baggage"))
                            .asInstanceOf(InstanceOfAssertFactories.map(Utf8.class, Utf8.class))
                            .hasEntrySatisfying(
                                    new Utf8("orders"),
                                    v -> assertThat((CharSequence) v).isEqualTo(new Utf8(baggageValue))
                            );
                });
    }
}
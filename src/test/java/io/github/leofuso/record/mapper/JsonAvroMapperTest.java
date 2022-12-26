package io.github.leofuso.record.mapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import io.github.leofuso.record.mapper.fixture.JsonParameterResolver;
import io.github.leofuso.record.mapper.fixture.SchemaParameterResolver;
import io.github.leofuso.record.mapper.fixture.annotation.JsonParameter;
import io.github.leofuso.record.mapper.fixture.annotation.SchemaParameter;
import io.github.leofuso.record.mapper.internal.JsonMapperFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.leofuso.obs.demo.events.Operation;
import io.github.leofuso.obs.demo.events.StatementLine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

@DisplayName("JsonAvroMapper Unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
class JsonAvroMapperTest {

    private static RecordMapper mapper;

    @BeforeAll
    static void setUp() {
        final RecordMapperFactory mapperFactory = RecordMapperFactory.get();
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

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThatRecord(record);
    }

    @Test
    @DisplayName(
            """
                    Given a friendly json template with a valid Schema and scrambled fields,
                    When converted to GenericData.Record,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf717843(
            @SchemaParameter(location = "statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v3.template.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThatRecord(record);
    }

    @Test
    @DisplayName(
            """
                    Given a friendly json template with a valid Schema and missing optional field,
                    When converted to GenericData.Record,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf717841(
            @SchemaParameter(location = "statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v4.template.json") String json
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThat(record)
                .isNotNull()
                .extracting(r -> r.get("amount"))
                .isNull();
    }

    @Test
    @DisplayName(
            """
                    Given a friendly json template with a valid Schema,
                    When converted to SpecificRecord,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf717842(
            @JsonParameter(location = "statement.line/statement-line.v3.template.json") String json
    ) {

        /* When */
        final StatementLine record = mapper.asRecord(json, StatementLine.class);

        /* Then */
        assertThatRecord(record);
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
    ) {

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(json, schema);

        /* Then */
        assertThatRecord(record);
    }

    @Test
    @DisplayName(
            """
                    Given a canonical-form json template with a valid Schema,
                    When converted to SpecificRecord,
                    Then all fields must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf717849(
            @JsonParameter(location = "statement.line/statement-line.v1.template.json") String json
    ) {

        /* When */
        final StatementLine record = mapper.asRecord(json, StatementLine.class);

        /* Then */
        assertThatRecord(record);
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
            @SchemaParameter(location = "statement-line.schema.avsc") Schema schema,
            @JsonParameter(location = "statement.line/statement-line.v1.template.json") String canonicalJson,
            @JsonParameter(location = "statement.line/statement-line.v2.template.json") String relaxedJson
    ) throws JsonProcessingException {

        /* Given */
        final ObjectMapper objectMapper = JsonMapperFactory.getInstance();
        final JsonNode expectedJsonNode = objectMapper.readTree(canonicalJson);
        final GenericData.Record record = mapper.asGenericDataRecord(relaxedJson, schema);

        /* When */
        final JsonNode actualJsonNode = mapper.asJsonNode(record);

        /* Then */
        assertThat(actualJsonNode)
                .isEqualTo(expectedJsonNode);
    }

    @Test
    @DisplayName(
            """
                    Given a SpecificRecord,
                    When converted to JsonNode,
                    Then the values must match.
                    """
    )
    void b8aec7e506ce410bb646f517cf71784g(
            @JsonParameter(location = "statement.line/statement-line.v1.template.json") String canonicalJson,
            @JsonParameter(location = "statement.line/statement-line.v2.template.json") String relaxedJson
    ) throws JsonProcessingException {

        /* Given */
        final ObjectMapper objectMapper = JsonMapperFactory.getInstance();
        final JsonNode expectedJsonNode = objectMapper.readTree(canonicalJson);

        final StatementLine record = mapper.asRecord(relaxedJson, StatementLine.class);

        /* When */
        final JsonNode actualJsonNode = mapper.asJsonNode(record);

        /* Then */
        assertThat(actualJsonNode)
                .isEqualTo(expectedJsonNode);
    }

    private static void assertThatRecord(final GenericRecord record) {
        assertThat(record)
                .isNotNull()
                .satisfies(input -> {

                    assertThat(input)
                            .extracting(i -> i.get("transaction"))
                            .isEqualTo(UUID.fromString("53a50a63-93ba-5ffd-b8cf-981a73947a7f"));

                    assertThat(input)
                            .extracting(i -> i.get("source"))
                            .asInstanceOf(type(GenericRecord.class))
                            .extracting(source -> source.get("namespace"))
                            .isEqualTo(UUID.fromString("74074428-7c4d-4922-b8fd-7c32e9ae499b"));

                    assertThat(input)
                            .extracting(i -> i.get("source"))
                            .asInstanceOf(type(GenericRecord.class))
                            .extracting(source -> source.get("domain"))
                            .satisfiesAnyOf(
                                    domain -> assertThat(domain)
                                            .isEqualTo(new Utf8("obs.demo")),
                                    domain -> assertThat(domain)
                                            .isEqualTo("obs.demo")
                            );

                    assertThat(input)
                            .extracting(i -> i.get("account"))
                            .isEqualTo(UUID.fromString("97175496-7cb2-468e-a812-015f7a44450f"));

                    assertThat(input)
                            .extracting(i -> i.get("operation"))
                            .satisfiesAnyOf(
                                    op -> assertThat(op)
                                            .isEqualTo(new GenericData.EnumSymbol(record.getSchema(), "CREDIT")),
                                    o -> assertThat(o)
                                            .isSameAs(Operation.CREDIT)
                            );


                    assertThat(input).extracting(i -> i.get("amount"))
                            .asInstanceOf(type(BigDecimal.class))
                            .usingComparator(BigDecimal::compareTo)
                            .isEqualTo(BigDecimal.valueOf(19565, 3));

                    assertThat(input)
                            .extracting(i -> i.get("competence"))
                            .isEqualTo(Instant.parse("2022-12-18T04:51:55.565970Z"));

                    assertThat(input)
                            .extracting(i -> i.get("date"))
                            .isEqualTo(LocalDate.parse("2022-12-22"));

                    assertThat(input)
                            .extracting(i -> i.get("time"))
                            .isEqualTo(LocalTime.parse("20:23:59.059"));

                    final String baggageValue =
                            "f1a4cfc3-b1a2-459d-ab0c-d7ac3cac42d0, 126cefe4-aab4-4bb6-9876-e245c834b0ac, " +
                                    "4475ad68-5c74-40bb-ae82-c79dfab3f149";

                    assertThat(input)
                            .extracting(i -> i.get("baggage"))
                            .asInstanceOf(InstanceOfAssertFactories.MAP)
                            .satisfiesAnyOf(
                                    map -> assertThat(map)
                                            .asInstanceOf(map(Utf8.class, Utf8.class))
                                            .hasEntrySatisfying(
                                                    new Utf8("orders"),
                                                    v -> assertThat((CharSequence) v)
                                                            .isEqualTo(new Utf8(baggageValue))
                                            ),
                                    map -> assertThat(map)
                                            .asInstanceOf(map(String.class, String.class))
                                            .hasEntrySatisfying(
                                                    "orders",
                                                    v -> assertThat(v)
                                                            .isEqualTo(baggageValue)
                                            )
                            );

                });
    }
}
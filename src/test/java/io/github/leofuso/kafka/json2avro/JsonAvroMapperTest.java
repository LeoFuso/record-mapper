package io.github.leofuso.kafka.json2avro;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.leofuso.kafka.json2avro.fixture.JsonParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.SchemaParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.annotation.JsonParameter;
import io.github.leofuso.kafka.json2avro.fixture.annotation.SchemaParameter;
import io.github.leofuso.kafka.json2avro.internal.ObjectMapperFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JsonAvroMapper Unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
class JsonAvroMapperTest {

    private JsonMapper mapper;

    @BeforeEach
    void setUp() {
        final JsonMapperFactory factory = JsonMapperFactory.get();
        mapper = factory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given a json template with a valid Schema,
                     when mapping to GenericDataRecord,
                     then all fields should match.
                    """
    )
    void b8aec7e506ce410bb646f517cf71784c(
            @JsonParameter(location = "statement-line.v2.template.json") String json,
            @SchemaParameter(location = "statement-line.schema.json") Schema schema
    ) {

        /* Given */
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        /* When */
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* Then */
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
                });
    }

    @Test
    @DisplayName(
            """
                    Given a Record,
                     when mapping to a Json String representation,
                     then the values must match
                    """
    )
    void b8aec7e506ce410bb646f517cf71784d(
            @JsonParameter(location = "statement-line.template.json") String json,
            @SchemaParameter(location = "statement-line.schema.json") Schema schema
    ) throws JsonProcessingException {

        /* Given */
        final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
        final JsonNode originalJsonNode = objectMapper.readTree(json);

        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* When */
        final String parsedJson = mapper.asJsonNode(record)
                .asText();
        final JsonNode parsedJsonNode = objectMapper.readTree(parsedJson);

        /* Then */
        assertThat(parsedJsonNode)
                .isEqualTo(originalJsonNode);
    }

    @Test
    @DisplayName(
            """
                    Given a Record,
                     when mapping to JsonNode,
                     then the values must match
                    """
    )
    void b8aec7e506ce410bb646f517cf71784f(
            @JsonParameter(location = "statement-line.template.json") String json,
            @SchemaParameter(location = "statement-line.schema.json") Schema schema
    ) throws JsonProcessingException {

        /* Given */
        final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
        final JsonNode originalJsonNode = objectMapper.readTree(json);

        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final GenericData.Record record = mapper.asGenericDataRecord(bytes, schema);

        /* When */
        final JsonNode parsedJsonNode = mapper.asJsonNode(record);

        /* Then */
        assertThat(parsedJsonNode)
                .isEqualTo(originalJsonNode);
    }
}
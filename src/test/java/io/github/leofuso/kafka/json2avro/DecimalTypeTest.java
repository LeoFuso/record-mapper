package io.github.leofuso.kafka.json2avro;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.leofuso.kafka.json2avro.fixture.JsonParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.SchemaParameterResolver;
import io.github.leofuso.kafka.json2avro.fixture.annotation.JsonParameter;
import io.github.leofuso.kafka.json2avro.fixture.annotation.SchemaParameter;
import io.github.leofuso.kafka.json2avro.instrument.bytecode.ByteCodeRewriter;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("LogicalType Decimal unit tests")
@ExtendWith({ SchemaParameterResolver.class, JsonParameterResolver.class })
public class DecimalTypeTest {

    private JsonMapper mapper;

    @BeforeEach
    void setUp() {
        ByteCodeRewriter.rewrite();
        final JsonMapperFactory mapperFactory = JsonMapperFactory.get();
        mapper = mapperFactory.produce();
    }

    @Test
    @DisplayName(
            """
                    Given a double-typed decimal value,
                    When converting to GenericData.Record,
                    Then should apply the corresponding value to BigDecimal amount field
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
}

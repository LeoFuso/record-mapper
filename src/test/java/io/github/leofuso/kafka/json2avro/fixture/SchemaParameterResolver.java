package io.github.leofuso.kafka.json2avro.fixture;

import java.lang.reflect.*;
import java.nio.charset.*;

import org.apache.avro.*;
import org.junit.jupiter.api.extension.*;

import io.github.leofuso.kafka.json2avro.fixture.annotation.*;

public class SchemaParameterResolver implements ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameter, ExtensionContext extension) throws ParameterResolutionException {
        final Parameter subject = parameter.getParameter();
        final Class<?> parameterType = subject.getType();
        return Schema.class.isAssignableFrom(parameterType);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.findAnnotation(SchemaParameter.class)
                .map(SchemaParameter::location)
                .map(location -> {
                    final byte[] bytes = ResourceFactory.load(location);
                    final String rawSchema = new String(bytes, StandardCharsets.UTF_8);
                    return new Schema.Parser().parse(rawSchema);
                })
                .orElse(null);
    }
}

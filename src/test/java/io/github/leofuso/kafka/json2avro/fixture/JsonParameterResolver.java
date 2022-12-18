package io.github.leofuso.kafka.json2avro.fixture;

import java.lang.reflect.*;
import java.nio.charset.*;

import org.junit.jupiter.api.extension.*;

import io.github.leofuso.kafka.json2avro.fixture.annotation.*;

public class JsonParameterResolver implements ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameter, ExtensionContext extension) throws ParameterResolutionException {
        final Parameter subject = parameter.getParameter();
        final Class<?> parameterType = subject.getType();
        return String.class.isAssignableFrom(parameterType) && parameter.isAnnotated(JsonParameter.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.findAnnotation(JsonParameter.class)
                .map(JsonParameter::location)
                .map(location -> {
                    final byte[] bytes = ResourceFactory.load(location);
                    return new String(bytes, StandardCharsets.UTF_8);
                })
                .orElse(null);
    }
}

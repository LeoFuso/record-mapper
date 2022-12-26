package io.github.leofuso.record.mapper.fixture;

import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import io.github.leofuso.record.mapper.fixture.annotation.SchemaParameter;


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

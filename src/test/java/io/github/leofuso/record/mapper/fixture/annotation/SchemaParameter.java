package io.github.leofuso.record.mapper.fixture.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.github.leofuso.record.mapper.fixture.JsonParameterResolver;


/**
 * An annotation aimed to pass metadata needed to the {@link JsonParameterResolver JsonParameter} be capable of instantiating a
 * {@link java.io.InputStream InputStream} holding the desired json value.
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SchemaParameter {

    /**
     * JSON-style template <em>location</em> to be loaded.
     *
     * @see JsonParameterResolver
     */
    String location() default "";

}


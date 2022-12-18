package io.github.leofuso.kafka.json2avro.fixture.annotation;


import java.lang.annotation.*;

import io.github.leofuso.kafka.json2avro.fixture.*;


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


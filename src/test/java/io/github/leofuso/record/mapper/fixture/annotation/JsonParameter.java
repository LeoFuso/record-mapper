package io.github.leofuso.record.mapper.fixture.annotation;


import java.lang.annotation.*;

import io.github.leofuso.record.mapper.fixture.JsonParameterResolver;


/**
 * An annotation aimed to pass metadata needed to the {@link JsonParameterResolver JsonParameter} be capable of instantiating a
 * {@link java.io.InputStream InputStream} holding the desired json value.
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface JsonParameter {

    /**
     * JSON-style template <em>location</em> to be loaded.
     *
     * @see JsonParameterResolver
     */
    String location() default "";

}


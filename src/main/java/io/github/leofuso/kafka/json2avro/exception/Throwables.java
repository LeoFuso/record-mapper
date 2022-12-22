package io.github.leofuso.kafka.json2avro.exception;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Simple utility class for handling exceptions and reflecting exceptions.
 *
 * <p>Only intended for internal use.
 */
public final class Throwables {


    private Throwables() {
        throw new UnsupportedOperationException("Throwable class should not be instanciated.");
    }

    /**
     * Handle the given reflection exception.
     * <p>Should only be called if no checked exception is expected to be thrown
     * by a target method, or if an error occurs while accessing a method or field.
     * <p>Throws the underlying RuntimeException or Error in case of an
     * InvocationTargetException with such a root cause. Throws an IllegalStateException with an appropriate message or
     * UndeclaredThrowableException otherwise.
     *
     * @param ex the reflection exception to handle
     */
    public static void handleReflectionException(final Throwable ex) {
        if (ex instanceof NoSuchMethodException) {
            throw new IllegalStateException("A Method was not found: " + ex.getMessage());
        }
        if (ex instanceof IllegalAccessException) {
            final String message = "Could not access this method or field: " + ex.getMessage();
            throw new IllegalStateException(message);
        }
        if (ex instanceof InvocationTargetException e) {
            /* Unwrapping root Exception */
            final Throwable targetException = e.getTargetException();
            handleReflectionException(targetException);
        }
        if (ex instanceof RuntimeException e) {
            throw e;
        }
        rethrowRuntimeException(ex);
    }

    /**
     * Rethrow the given {@link Throwable exception}, which is presumably the
     * <em>target exception</em> of an {@link InvocationTargetException}.
     * Should only be called if no checked exception is expected to be thrown by the target method.
     * <p>Rethrows the underlying exception cast to a {@link RuntimeException} or
     * {@link Error} if appropriate; otherwise, throws an {@link java.lang.reflect.UndeclaredThrowableException}.
     *
     * @param ex the exception to rethrow
     * @throws RuntimeException the rethrown exception
     */
    public static void rethrowRuntimeException(final Throwable ex) {
        if (ex instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (ex instanceof Error error) {
            throw error;
        }
        throw new UndeclaredThrowableException(ex);
    }

    /**
     * Rethrow the given {@link Throwable exception}, which is presumably the
     * <em>target exception</em> of an {@link InvocationTargetException}.
     * Should only be called if no checked exception is expected to be thrown by the target method.
     * <p>Rethrows the underlying exception cast to an {@link Exception} or
     * {@link Error} if appropriate; otherwise, throws an {@link java.lang.reflect.UndeclaredThrowableException}.
     *
     * @param throwable the exception to rethrow
     * @throws Exception the rethrown exception (in case of a checked exception)
     */
    public static void rethrowException(final Throwable throwable) throws Exception {
        if (throwable instanceof Exception exception) {
            throw exception;
        }
        if (throwable instanceof Error error) {
            throw error;
        }
        throw new UndeclaredThrowableException(throwable);
    }

}
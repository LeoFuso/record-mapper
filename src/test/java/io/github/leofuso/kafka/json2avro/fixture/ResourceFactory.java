package io.github.leofuso.kafka.json2avro.fixture;

import javax.annotation.*;

import java.io.*;
import java.net.*;

public class ResourceFactory {

    @Nonnull
    public static byte[] load(final String location) {

        final URL resource = ResourceFactory.class
                .getClassLoader()
                .getResource(location);

        if (resource == null) {
            throw new NullPointerException("ResourceFactory was unnable to locate [%s]".formatted(location));
        }

        try (final InputStream stream = resource.openStream()) {
            return stream.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

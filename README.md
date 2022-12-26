# Record Mapper

[![](https://jitpack.io/v/LeoFuso/record-mapper.svg)](https://jitpack.io/#LeoFuso/record-mapper)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.leofuso/record-mapper/badge.svg#)](https://maven-badges.herokuapp.com/maven-central/io.github.leofuso/record-mapper)


RecordMapper provides functionality for reading and writing Avro-compatible Records to or from String JSON equivalent.

It ships with two kinds of _Mappers_:
1. Using a more relaxed version of the existent JSON encoders, referenced as an _Enhanced_ version;
   * _Enhanced_ provides a collection of customized conversions to all LogicalTypes, in a friendly manner.
2. Using the pre-existent JSON encoders that ships with the Avro dependency;

If you have [**ByteBuddy**](https://bytebuddy.net/) in your classpath, the _Enhanced_ version is automatically applied.

## Motivation

Apache Avro ships with some advanced and efficient tools for reading and writing binary Avro format, 
but their support for JSON to Avro conversion is unfortunately limited and requires wrapping fields 
with type declarations if you have some optional fields in your schema.

This simple library is supposed to help with the not-so-intuitive way of writing Avro-compatible Records. 
By using a more approuchable JSON structure and feeding it to this library's main Api,
one can generate all sorts of test data for a development environment.

This library is a best-effort drop-in-replacement to the [Allegro's Json to Avro converter](https://github.com/allegro/json-avro-converter).

## Usage

### Features

* Conversion from JSON-string to binary Avro (wrapped in a ByteBuffer);
* Conversion from JSON-string to GenericData.Record;
* Conversion from JSON-string to Avro generated Java classes (SpecificRecords);
* Conversion from Avro Records to JsonNode;

### Dependencies

There are *almost* no transitive dependencies, so you'll need to have some version
of the [Apache Avro project](https://avro.apache.org/project/download/) in your classpath.

The minimal Gradle configuration is as follows:

```groovy 

dependencies {
    implementation 'org.apache.avro:avro:1.11.1'
    implementation 'io.github.leofuso.record-mapper:1.0.0'
}

```
Note that the minimal configuration doesn't come with the _Enhanced_ conversions.

By applying the _Enhanced_ strategy, either by Gradle or by providing a **ByteBuddy** dependency on the classpath:
```groovy
dependencies {
    implementation 'org.apache.avro:avro:1.11.1'
    implementation ('io.github.leofuso.record-mapper:1.0.0') {
        capabilities {
            requireCapability 'io.github.leofuso:record-mapper-enhanced'
        }
    }
}
```

You can now convert canonical JSON structures into a compatible-Avro Record, using the additional custom LogicalType converters.

A Java example using **Decimal type** conversion:

```java
import io.github.leofuso.record.mapper.*;

class Mapper {
    
    public static void main(String[] args) {
        
        final RecordMapperFactory mapperFactory = RecordMapperFactory.get();
        final RecordMapper mapper = mapperFactory.produce();
        
        final String rawSchema = """
                {
                    "type": "record",
                    "name": "Decimal",
                    "namespace": "io.github.leofuso.record.mapper.test",
                    "doc": "A simple Record containing only a decimal field.",
                    "fields": [
                        {
                            "name": "amount",
                            "type": {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 15,
                                "scale": 3
                            },
                            "doc": "The amount. It can be positive, negative or zero."
                        }
                    ]
                }
                """;
        
        final String canonicalJson = """
                {
                    "amount": "\u004c\u006d"
                }
                """;

        final String frieldyJson = """
                {
                    "amount": 19.565
                }
                """;

        final GenericData.Record enhancedStrategyRecord = mapper.asGenericDataRecord(frieldyJson, new Schema.Parser().parse(rawSchema));
        final GenericData.Record defaultStrategyRecord = mapper.asGenericDataRecord(canonicalJson, new Schema.Parser().parse(rawSchema));
        
        assertThat(both(enhancedStrategyRecord, defaultStrategyRecord))
                .isNotNull()
                .extracting(i -> i.get("amount"))
                .asInstanceOf(InstanceOfAssertFactories.type(BigDecimal.class))
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(BigDecimal.valueOf(19.565));
    }
}
```

Note that, since this strategy relies on **ByteBuddy** for its instrumentation, naturally, it carries its limitations as well.
You cannot use the instrumentation after referring to the instrumented code. If, for some reason, you have references of 
`org.apache.avro.io.ResolvingDecoder`, `org.apache.avro.io.ValidatingDecoder`, `org.apache.avro.io.JsonDecoder`, etc., you may need to
produce the `RecordMapperFactory` before it.

### Enhanced Conversions

There's a conversion from all Logical Types, with different rules. You can check them all by looking at the unit tests of this project.
Don't exitate to ask for some more, either by proving a Merge Request or by creating an issue.

Contributions are more than welcome!
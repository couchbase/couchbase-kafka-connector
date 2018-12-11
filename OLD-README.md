# This project is obsolete

This project is superseded by [kafka-connect-couchbase](https://github.com/couchbase/kafka-connect-couchbase)
which integrates with the Kafka Connect framework.


# Couchbase Kafka Connector

Welcome to the official Couchbase Kafka connector! It provides functionality to direct a stream of events from Couchbase
Server to Kafka.

You can read the quickstart guide below or consult the documentation here: http://developer.couchbase.com/documentation/server/4.1/connectors/kafka-2.0/kafka-intro.html

The issue tracker can be found at [https://issues.couchbase.com/browse/KAFKAC](https://issues.couchbase.com/browse/KAFKAC).

## Quickstart

A sample `build.gradle`:

```groovy
apply plugin: 'java'

repositories {
    mavenCentral()
    maven { url { "http://files.couchbase.com/maven2" } }
    mavenLocal()
}

dependencies {
    compile(group: 'com.couchbase.client', name: 'kafka-connector', version: '2.0.1')
}
```

Using the library is pretty easy. Let's say we would like to receive every modification from the Couchbase Server
and send to Kafka only the document body (by default the connector serializes the document body and metadata to JSON). To achieve that, you need to define a filter class that allows only instances of `MutationMessage` to pass through:

```java
package example;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.kafka.DCPEvent;
import com.couchbase.kafka.filter.Filter;

public class SampleFilter implements Filter {
    @Override
    public boolean pass(final DCPEvent dcpEvent) {
        return dcpEvent.message() instanceof MutationMessage;
    }
}
```

And you also need an encoder class, which takes document value converts it to byte array:

```java
package example;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.DCPEvent;
import com.couchbase.kafka.coder.AbstractEncoder;
import kafka.utils.VerifiableProperties;

public class SampleEncoder extends AbstractEncoder {
    public SampleEncoder(final VerifiableProperties properties) {
        super(properties);
    }

    @Override
    public byte[] toBytes(final DCPEvent dcpEvent) {
        MutationMessage message = (MutationMessage)dcpEvent.message();
        return message.content().toString(CharsetUtil.UTF_8).getBytes();
    }
}
```

That essentially is enough to setup a Couchbase-Kafka bridge:

```java
package example;

import com.couchbase.kafka.CouchbaseKafkaConnector;
import com.couchbase.kafka.CouchbaseKafkaEnvironment;
import com.couchbase.kafka.DefaultCouchbaseKafkaEnvironment;

public class Example {
    public static void main(String[] args) {
        DefaultCouchbaseKafkaEnvironment.Builder builder =
                (DefaultCouchbaseKafkaEnvironment.Builder) DefaultCouchbaseKafkaEnvironment
                        .builder()
                        .kafkaFilterClass("example.SampleFilter")
                        .kafkaValueSerializerClass("example.SampleEncoder")
                        .kafkaTopic("default")
                        .kafkaZookeeperAddress("kafka1.vagrant")
                        .couchbaseNodes("couchbase1.vagrant")
                        .couchbaseBucket("default")
                        .dcpEnabled(true);
        CouchbaseKafkaConnector connector = CouchbaseKafkaConnector.create(builder.build());
        connector.run();
    }
}
```

It is also possible to start with some known state or to watch a limited set of partitions. The example below will stream
only partition 115 starting from the beginning (see also `currentState()` and `loadState()` helpers).

```java
ConnectorState startState = connector.startState(115);
ConnectorState endState = connector.endState(115);
connector.run(startState, endState);
```

The `couchbase1.vagrant` and `kafka1.vagrant` addresses above are the locations of Couchbase Server and Kafka respectively,
which could be easily set up using provisioning scripts from the `env/` directory. Just navigate there and run `vagrant up`.
Vagrant scripts using Ansible ([installation guide](http://docs.ansible.com/intro_installation.html)).

## License

Copyright 2015 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

# Couchbase Kafka Connector

Welcome to the official couchbase kafka connector! It provides functionality to direct stream of events from Couchbase
Server to Kafka. It is still under development, so use with care and open issues if you come across them. Its issue 
tracker located at [https://issues.couchbase.com/browse/KAFKAC](https://issues.couchbase.com/browse/KAFKAC).

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
    compile(group: 'com.couchbase.client', name: 'kafka-connector', version: '1.0.0-dp1')
}
```

The usage of the library is pretty easy. Lets say we would like to receive every modification from the Couchbase Server 
and send to Kafka only document body (by default connector serializes document body and metadata to JSON). To achieve
that you need to define filter class which allow only instances of `MutationMessage` to pass through:

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

And encoder class, which takes document value converts it to byte array:

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

That essentially enough to setup Couchbase-Kafka bridge:

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

It is also possible to start with some known state or watching reduced set of partitions. Example below will stream
only partition 115 starting from beginning.

```java
BucketStreamAggregatorState state = new BucketStreamAggregatorState("test");
state.put(new BucketStreamState((short)115, 0, 0, 0xffffffff, 0, 0xffffffff));
connector.run(state, RunMode.RESUME);
```

The `couchbase1.vagrant` and `kafka1.vagrant` addresses above are locations of Couchbase Server and Kafka correspondingly,
which could be easily set up using provisioning scripts from `env/` directory. Just navigate there and run `vagrant up`.
Vagrant scripts using ansible ([installation guide](http://docs.ansible.com/intro_installation.html)).

## License

Copyright 2015 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

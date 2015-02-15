/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.kafka;

import com.couchbase.client.core.env.DefaultCoreEnvironment;

/**
 * @author Sergey Avseyev
 */
public class DefaultCouchbaseKafkaEnvironment extends DefaultCoreEnvironment implements CouchbaseKafkaEnvironment {
    private static final String KAFKA_KEY_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private static final String KAFKA_VALUE_SERIALIZER_CLASS = "com.couchbase.kafka.coder.JsonEncoder";
    private static final String KAFKA_FILTER_CLASS = "com.couchbase.kafka.filter.MutationsFilter";
    private static final int KAFKA_EVENT_BUFFER_SIZE = 16384;
    private final String kafkaKeySerializerClass;
    private final String kafkaFilterClass;
    private final String kafkaValueSerializerClass;
    private final int kafkaEventBufferSize;

    public static DefaultCouchbaseKafkaEnvironment create() {
        return new DefaultCouchbaseKafkaEnvironment(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    protected DefaultCouchbaseKafkaEnvironment(final Builder builder) {
        super(builder);

        if (!dcpEnabled()) {
            throw new IllegalStateException("Kafka integration cannot work without DCP enabled.");
        }

        kafkaKeySerializerClass = stringPropertyOr("kafka.keySerializerClass", builder.kafkaKeySerializerClass());
        kafkaValueSerializerClass = stringPropertyOr("kafka.valueSerializerClass", builder.kafkaValueSerializerClass());
        kafkaFilterClass = stringPropertyOr("kafka.filterClass", builder.kafkaFilterClass());
        kafkaEventBufferSize = intPropertyOr("kafka.eventBufferSize", builder.kafkaEventBufferSize());
    }

    @Override
    public String kafkaValueSerializerClass() {
        return kafkaValueSerializerClass;
    }

    @Override
    public String kafkaKeySerializerClass() {
        return kafkaKeySerializerClass;
    }

    @Override
    public String kafkaFilterClass() {
        return kafkaFilterClass;
    }

    @Override
    public int kafkaEventBufferSize() {
        return kafkaEventBufferSize;
    }

    public static class Builder extends DefaultCoreEnvironment.Builder implements CouchbaseKafkaEnvironment {
        private String kafkaKeySerializerClass = KAFKA_KEY_SERIALIZER_CLASS;
        private String kafkaValueSerializerClass = KAFKA_VALUE_SERIALIZER_CLASS;
        private int kafkaEventBufferSize = KAFKA_EVENT_BUFFER_SIZE;
        private String kafkaFilterClass = KAFKA_FILTER_CLASS;

        @Override
        public String kafkaValueSerializerClass() {
            return kafkaValueSerializerClass;
        }

        @Override
        public String kafkaKeySerializerClass() {
            return kafkaKeySerializerClass;
        }

        @Override
        public String kafkaFilterClass() {
            return kafkaFilterClass;
        }

        @Override
        public int kafkaEventBufferSize() {
            return kafkaEventBufferSize;
        }

        public Builder kafkaValueSerializerClass(final String className) {
            this.kafkaValueSerializerClass = className;
            return this;
        }

        public Builder kafkaKeySerializerClass(final String className) {
            this.kafkaKeySerializerClass = className;
            return this;
        }

        public Builder kafkaFilterClass(final String className) {
            this.kafkaFilterClass = className;
            return this;
        }

        public Builder kafkaEventBufferSize(final int eventBufferSize) {
            this.kafkaEventBufferSize = eventBufferSize;
            return this;
        }

        @Override
        public DefaultCouchbaseKafkaEnvironment build() {
            return new DefaultCouchbaseKafkaEnvironment(this);
        }
    }
}

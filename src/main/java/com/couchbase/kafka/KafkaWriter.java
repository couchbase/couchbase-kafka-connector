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

import com.couchbase.client.deps.com.lmax.disruptor.EventHandler;
import com.couchbase.kafka.filter.Filter;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * {@link KafkaWriter} is in charge of filtering and routing events to the Kafka cluster.
 *
 * @author Sergey Avseyev
 */
public class KafkaWriter implements EventHandler<DCPEvent> {

    private final Producer<String, DCPEvent> producer;
    private final String topic;
    private final Filter filter;

    /**
     * Creates a new {@link KafkaWriter}.
     *
     * @param topic the topic, where events should be published.
     * @param producer the kafka producer object.
     * @param filter the filter to select events to publish.
     */
    public KafkaWriter(final String topic, final Producer<String, DCPEvent> producer, final Filter filter) {
        this.topic = topic;
        this.producer = producer;
        this.filter = filter;
    }

    /**
     * Handles {@link DCPEvent}s that come into the response RingBuffer.
     */
    @Override
    public void onEvent(final DCPEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        if (filter.pass(event)) {
            KeyedMessage<String, DCPEvent> payload =
                    new KeyedMessage<String, DCPEvent>(topic, event.key(), event);
            producer.send(payload);
        }
    }
}

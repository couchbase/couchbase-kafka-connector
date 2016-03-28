/**
 * Copyright (C) 2016 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.kafka.state;

import com.couchbase.client.core.message.kv.MutationToken;

/**
 * This class represents state of DCP stream.
 *
 * @since 1.3.0
 */
public class StreamState {
    private final MutationToken token;

    public StreamState(MutationToken token) {
        this.token = token;
    }

    public StreamState(short partition, long vbucketUUID, long sequenceNumber) {
        this.token = new MutationToken(partition, vbucketUUID, sequenceNumber, null);
    }

    /**
     * A unique identifier that is generated that is assigned to each VBucket.
     * This number is generated on an unclean shutdown or when a VBucket becomes
     * active.
     *
     * @return the stream vbucketUUID.
     */
    public long vbucketUUID() {
        return token.vbucketUUID();
    }

    /**
     * Specified the last by sequence number that has been seen by the consumer.
     *
     * @return the stream last sequence number.
     */
    public long sequenceNumber() {
        return token.sequenceNumber();
    }

    /**
     * The partition number (vBucket), to which this state belongs.
     *
     * @return the stream partition number.
     */
    public short partition() {
        return (short) token.vbucketID();
    }

}

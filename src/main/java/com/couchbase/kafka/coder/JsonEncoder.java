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

package com.couchbase.kafka.coder;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParseException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.DCPEvent;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * The {@link JsonEncoder} converts events from Couchbase to JSON.
 * 
 * If the document body looks like JSON, it inserts it as a sub-tree of the resulting object,
 * otherwise, it puts it as a String.
 *
 * @author Sergey Avseyev
 */
public class JsonEncoder extends AbstractEncoder {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonEncoder.class);

    public JsonEncoder(final VerifiableProperties properties) {
        super(properties);
    }

    /**
     * Encodes {@link DCPEvent} to JSON object.
     *
     * @param value event from Couchbase.
     * @return JSON object in form of byte array.
     */
    @Override
    public byte[] toBytes(final DCPEvent value) {
        try {
            ObjectNode message = MAPPER.createObjectNode();
            if (value.message() instanceof MutationMessage) {
                MutationMessage mutation = (MutationMessage) value.message();
                message.put("event", "mutation");
                message.put("key", mutation.key());
                message.put("expiration", mutation.expiration());
                message.put("flags", mutation.flags());
                message.put("cas", mutation.cas());
                message.put("lockTime", mutation.lockTime());
                message.put("bySeqno", mutation.bySequenceNumber());
                message.put("revSeqno", mutation.revisionSequenceNumber());
                try {
                    message.set("content", MAPPER.readTree(mutation.content().toString(CharsetUtil.UTF_8)));
                } catch (JsonParseException e) {
                    message.put("content", mutation.content().toString(CharsetUtil.UTF_8));
                }
            } else if (value.message() instanceof RemoveMessage) {
                RemoveMessage mutation = (RemoveMessage) value.message();
                message.put("event", "removal");
                message.put("key", mutation.key());
                message.put("cas", mutation.cas());
                message.put("bySeqno", mutation.bySequenceNumber());
                message.put("revSeqno", mutation.revisionSequenceNumber());
            }
            return message.toString().getBytes();
        } catch (IOException ex) {
            LOGGER.warn("Error while encoding DCP message", ex);
        }
        return new byte[]{};
    }
}

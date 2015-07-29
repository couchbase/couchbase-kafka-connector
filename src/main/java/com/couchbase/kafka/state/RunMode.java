package com.couchbase.kafka.state;

/**
 * @author Sergey Avseyev
 */
public enum RunMode {
    /**
     * Resume the streams using specified state.
     */
    RESUME,
    /**
     * Load last known state using serializer, and resume streams.
     */
    LOAD_AND_RESUME
}

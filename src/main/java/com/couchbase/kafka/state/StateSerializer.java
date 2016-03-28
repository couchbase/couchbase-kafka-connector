package com.couchbase.kafka.state;

/**
 * @author Sergey Avseyev
 */
public interface StateSerializer {
    void dump(ConnectorState connectorState);

    void dump(ConnectorState connectorState, short partition);

    ConnectorState load(ConnectorState connectorState);

    StreamState load(ConnectorState connectorState, short partition);
}

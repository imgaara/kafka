package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface HasNextCondition {
    boolean hasNext(final KeyValueIterator<Bytes, ?> iterator);
}
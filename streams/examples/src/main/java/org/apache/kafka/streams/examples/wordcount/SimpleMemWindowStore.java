package org.apache.kafka.streams.examples.wordcount;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.WindowStoreIteratorWrapper;

public class SimpleMemWindowStore implements WindowStore<Bytes, byte[]> {

    private static final int SUFFIX_SIZE = WindowStoreUtils.TIMESTAMP_SIZE + WindowStoreUtils.SEQNUM_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

    static class Segment extends InMemoryKeyValueStore<Bytes, byte[]> {
        long id;

        public Segment(String name,long id) {
            super(name, Serdes.Bytes(), Serdes.ByteArray());
            this.id = id;
        }
    }

    Map<Long, Segment> segments = new HashMap<>();
    private final String name;
    private final int numSegments;
    private final long segmentInterval;
    private long minSegmentId = Long.MAX_VALUE;
    private long maxSegmentId = -1L;
    private ProcessorContext context;
    private boolean open;
    private WindowKeySchema keySchema = new

    public SimpleMemWindowStore(String name, int numSegments, long segmentInterval) {
        this.name = name;
        this.numSegments = numSegments;
        this.segmentInterval = segmentInterval;
    }

    private Segment getSegment(long segmentId) {
        final Segment segment = segments.get(segmentId % numSegments);
        if (!isSegment(segment, segmentId)) {
            return null;
        }
        return segment;
    }

    private long segmentId(final long timestamp) {
        return timestamp / segmentInterval;
    }

    private boolean isSegment(final Segment segment, long segmentId) {
        return segment != null && segment.id == segmentId;
    }

    String segmentName(final long segmentId) {
        return name + "." + segmentId * segmentInterval;
    }

    private Segment getOrCreateSegment(final long segmentId, final ProcessorContext context) {
        if (segmentId > maxSegmentId - numSegments) {
            final long key = segmentId % numSegments;
            final Segment segment = segments.get(key);
            if (!isSegment(segment, segmentId)) {
                cleanup(segmentId);
            }
            Segment newSegment = new Segment(segmentName(segmentId), segmentId);
            Segment previousSegment = segments.putIfAbsent(key, newSegment);
            if (previousSegment == null) {
                newSegment.init(context, null);
                maxSegmentId = segmentId > maxSegmentId ? segmentId : maxSegmentId;
                minSegmentId = segmentId < minSegmentId ? segmentId : minSegmentId;
            }
            return previousSegment == null ? newSegment : previousSegment;
        } else {
            return null;
        }
    }

    private void cleanup(final long segmentId) {
        final long oldestSegmentId = maxSegmentId < segmentId
                                     ? segmentId - numSegments
                                     : maxSegmentId - numSegments;

        for (Map.Entry<Long, Segment> segmentEntry : segments.entrySet()) {
            final Segment segment = segmentEntry.getValue();
            if (segment != null && segment.id <= oldestSegmentId) {
                segments.remove(segmentEntry.getKey());
                segment.close();
            }
        }
        if (oldestSegmentId > minSegmentId) {
            minSegmentId = oldestSegmentId + 1;
        }
    }

    private Segment getSegmentForTimestamp(final long timestamp) {
        return getSegment(segmentId(timestamp));
    }

    @Override
    public void put(Bytes key, byte[] value) {
    }

    @Override
    public void put(Bytes key, byte[] value, long timestamp) {
        final long segmentId = segmentId(timestamp);
        final Segment segment = getOrCreateSegment(segmentId, context);
        if (segment != null) {
            segment.put(key, value);
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;
        open = true;
    }

    @Override
    public void flush() {
        // no need
    }

    @Override
    public void close() {
        segments.entrySet().forEach(s -> s.getValue().close());
        segments.clear();
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private List<Segment> segments(final long timeFrom, final long timeTo) {
        final long segFrom = Math.max(minSegmentId, segmentId(Math.max(0L, timeFrom)));
        final long segTo = Math.min(maxSegmentId, segmentId(Math.min(maxSegmentId * segmentInterval, Math.max(0, timeTo))));

        final List<Segment> segments = new ArrayList<>();
        for (long segmentId = segFrom; segmentId <= segTo; segmentId++) {
            Segment segment = getSegment(segmentId);
            if (segment != null && segment.isOpen()) {
                try {
                    segments.add(segment);
                } catch (InvalidStateStoreException ise) {
                    // segment may have been closed by streams thread;
                }
            }
        }
        return segments;
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
        final List<Segment>
                searchSpace = segments(timeFrom, timeTo);

        final Bytes binaryFrom = keySchema.lowerRange(keyFrom, from);
        final Bytes binaryTo = keySchema.upperRange(keyTo, to);

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(keyFrom, keyTo, from, to),
                                   binaryFrom, binaryTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(key, timeFrom, timeTo);
        return WindowStoreIteratorWrapper.bytesIterator(bytesIterator, serdes, windowSize).valuesIterator();
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final List<org.apache.kafka.streams.state.internals.Segment> searchSpace = segments.allSegments();

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE),
                                   null, null);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
        final List<Segment> searchSpace = segments(timeFrom, timeTo);

        return new SegmentIterator(searchSpace.iterator(),
                                   hasNextCondition(null, null, timeFrom, timeTo),
                                   null, null);
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        final byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
                                           .putLong(to)
                                           .putInt(Integer.MAX_VALUE)
                                           .array();

        return OrderedBytes.upperRange(key, maxSuffix);
    }

    public Bytes lowerRange(final Bytes key, final long from) {
        return OrderedBytes.lowerRange(key, MIN_SUFFIX);
    }

    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        return WindowStoreUtils.toBinaryKey(key, Math.max(0, from), 0, serdes);
    }

    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        return WindowStoreUtils.toBinaryKey(key, to, Integer.MAX_VALUE, serdes);
    }

    public long segmentTimestamp(final Bytes key) {
        return WindowStoreUtils.timestampFromBinaryKey(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Bytes keyBytes = WindowStoreUtils.bytesKeyFromBinaryKey(bytes.get());
                    final long time = WindowStoreUtils.timestampFromBinaryKey(bytes.get());
                    if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                        && time >= from
                        && time <= to) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            }
        };
    }
}

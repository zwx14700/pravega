/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.test.common.CounterClock;

import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MockSegmentStreamFactory implements SegmentInputStreamFactory, SegmentOutputStreamFactory, SegmentMetadataClientFactory {

    private final CounterClock clock = new CounterClock(ZoneId.systemDefault(), 42L);

    private final Map<Segment, MockSegmentIoStreams> segments = new ConcurrentHashMap<>();

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId,
                                                                Consumer<Segment> segmentSealedCallback,
                                                                EventWriterConfig config) {
        throw new UnsupportedOperationException();
    }

    public MockSegmentIoStreams getMockStream(Segment segment) {
        MockSegmentIoStreams streams = new MockSegmentIoStreams(segment, clock);
        segments.putIfAbsent(segment, streams);
        return segments.get(segment);
    }
    
    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback, EventWriterConfig config) {
        return getMockStream(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, int bufferSize) {
        return createInputStreamForSegment(segment);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment) {
        return getMockStream(segment);
    }

    @Override
    public SegmentMetadataClient createSegmentMetadataClient(Segment segment) {
        return getMockStream(segment);
    }
}

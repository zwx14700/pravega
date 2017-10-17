/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.EventReadImpl;
import lombok.SneakyThrows;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Mock EventStreamReader.
 */
public class EventStreamReaderMock<T> implements EventStreamReader<T> {
    BlockingQueue<EventRead<T>> queue;

    public EventStreamReaderMock(BlockingQueue<EventRead<T>> queue) {
        this.queue = queue;
    }

    @Override
    @SneakyThrows(value = InterruptedException.class)
    public EventRead<T> readNextEvent(long timeout) throws ReinitializationRequiredException {
        EventRead<T> eventRead = queue.poll(timeout, TimeUnit.MILLISECONDS);
        if (eventRead == null) {
            return new EventReadImpl<>(null, null, null, null, null, null);
        }
        return eventRead;
    }

    @Override
    public ReaderConfig getConfig() {
        return null;
    }

    @Override
    public T read(EventPointer pointer) throws NoSuchEventException {
        return null;
    }

    @Override
    public void close() {
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.util.concurrent.ScheduledExecutorService;
/**
 * An event that was read from a stream, a checkpoint marker if one has been requested, or a watermark
 * if the reader's time domain is {@code IngestionTime}.
 * <p>
 * A checkpoint is an indication that the reading application should persist its state to durable storage
 * before reading further. A checkpoint also represents a point where events waiting to be read may be
 * rebalanced among the readers in a group. So before a checkpoint one reader might be handling all of the
 * events sent with routing key X but afterwards it may be a different reader.
 * <p>
 * A watermark is an indication that a certain point in time has passed on the time clock (of the configured
 * time domain), based on the current position in the stream.
 *
 * @param <T> The type of the event.
 */
public interface EventRead<T> {

    /**
     * A boolean indicating if this is an event.
     *
     * @return true if this is an event.
     */
    default boolean isEvent() {
        return !isCheckpoint() && !isWatermark();
    }

    /**
     * Returns the event that is wrapped in this EventRead or null a timeout occurred or if a checkpoint was requested.
     *
     * @return The event itself.
     */
    T getEvent();

    /**
     * The position in the stream that represents where the reader is immediately following this
     * event. It is useful to store this so that
     * {@link ReaderGroup#readerOffline(String, Position)} can be called if the reader dies.
     *
     * @return Position of the event
     */
    Position getPosition();

    /**
     * Returns a pointer object for the event read. The event pointer enables a random read of the
     * event at a future time.
     *
     * @return Pointer to an event
     */
    EventPointer getEventPointer();

    /**
     * A boolean indicating if this is a checkpoint. In which case {@link #getCheckpointName()} will be non-null
     * and {@link #getEvent()} will be null.
     * 
     * @return true if this is a checkpoint.
     */
    boolean isCheckpoint();
    
    /**
     * If a checkpoint has been requested this will return the checkpointName passed to
     * {@link ReaderGroup#initiateCheckpoint(String,ScheduledExecutorService)} otherwise this will return null.
     * 
     * @return The name of the checkpoint
     */
    String getCheckpointName();

    /**
     * A boolean indicating if this is a watermark. In which case {@link #getWatermark()} will be non-null
     * and {@link #getEvent()} will be null.
     *
     * @return true if this is a watermark.
     */
    boolean isWatermark();

    /**
     * Returns a watermark representing the point in time (on the time clock) that was reached in the stream.
     *
     * @return The watermark time.
     */
    Long getWatermark();
}

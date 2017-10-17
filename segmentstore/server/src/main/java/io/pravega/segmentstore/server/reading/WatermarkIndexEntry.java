/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.util.SortedIndex;

/**
 * An entry in the Read Index with a watermark at a particular offset.
 */
final class WatermarkIndexEntry implements SortedIndex.IndexEntry {
    //region Members

    private final long streamSegmentOffset;
    private final long watermark;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param watermark The watermark value.
     * @throws IllegalArgumentException if the offset is a negative number.
     * @throws IllegalArgumentException if the length is a negative number.
     */
    WatermarkIndexEntry(long streamSegmentOffset, long watermark) {
        Preconditions.checkArgument(streamSegmentOffset >= -1, "streamSegmentOffset must be -1 or a non-negative number.");

        this.streamSegmentOffset = streamSegmentOffset;
        this.watermark = watermark;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the StreamSegment offset for this entry.
     */
    long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the watermark associated with the offset.
     */
    long getWatermark() {
        return this.watermark;
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, Watermark = %d", this.streamSegmentOffset, getWatermark());
    }

    @Override
    public long key() {
        return this.streamSegmentOffset;
    }

    //endregion
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.segmentstore.storage.NoAppendSyncStorage;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A layer on top of a general SyncStorage implementation that allows rolling Segments on a size-based policy and truncating
 * them at various offsets.
 *
 * Every Segment that is created using this Storage is made up of a Header and zero or more SegmentChunks.
 * * The Header contains the Segment's Rolling Policy, as well as an ordered list of Offset-to-SegmentChunk pointers for
 * all the SegmentChunks in the Segment.
 * * The SegmentChunks contain data that their Segment is made of. A SegmentChunk starting at offset N with length L contains
 * data for offsets [N,N+L) of the Segment.
 * * A Segment is considered to exist if it has a non-empty Header and if its last SegmentChunk exists. If it does not have
 * any SegmentChunks (freshly created), it is considered to exist.
 * * A Segment is considered to be Sealed if its Header is sealed.
 *
 * A note about compatibility:
 * * The RollingStorage wrapper is fully compatible with data and Segments that were created before RollingStorage was
 * applied. That means that it can access and modify existing Segments that were created without a Header, but all new
 * Segments will have a Header. As such, there is no need to do any sort of migration when starting to use this class.
 * * Should the RollingStorage need to be discontinued without having to do a migration:
 * ** The create() method should be overridden (in a derived class) to create new Segments natively (without a Header).
 * ** The concat() method should be overridden (in a derived class) to not convert Segments without Header into Segments
 * with Header.
 * ** Existing Segments (made up of Header and multi-SegmentChunks) can still be accessed by means of this class.
 */
@Slf4j
public class NoAppendRollingStorage implements SyncStorage {

    //region Members

    private final NoAppendSyncStorage baseStorage;
    private final SegmentRollingPolicy defaultRollingPolicy;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingStorage class with a default SegmentRollingPolicy set to NoRolling.
     *
     * @param baseStorage          A SyncStorage that will be used to execute operations.
     */
    public NoAppendRollingStorage(NoAppendSyncStorage baseStorage) {
        this(baseStorage, SegmentRollingPolicy.ALWAYS_ROLLING);
    }

    /**
     * Creates a new instance of the RollingStorage class.
     *
     * @param baseStorage          A SyncStorage that will be used to execute operations.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     */
    public NoAppendRollingStorage(NoAppendSyncStorage baseStorage, SegmentRollingPolicy defaultRollingPolicy) {
        this.baseStorage = Preconditions.checkNotNull(baseStorage, "baseStorage");
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.baseStorage.close();
            log.info("Closed");
        }
    }

    //endregion

    //region ReadOnlyStorage Implementation

    @Override
    public void initialize(long containerEpoch) {
        this.baseStorage.initialize(containerEpoch);
    }

    @Override
    public SegmentHandle openRead(String segmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", segmentName);
        val handle = openHandle(segmentName, true);
        LoggerHelpers.traceLeave(log, "openRead", traceId, handle);
        return handle;
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        val h = asReadableHandle(handle);
        long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
        ensureNotDeleted(h);
        Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        if (h.isReadOnly() && !h.isSealed() && offset + length > h.length()) {
            // We have a non-sealed read-only handle. It's possible that the SegmentChunks may have been modified since
            // the last time we refreshed it, and we received a request for a read beyond our last known offset. Reload
            // the handle before attempting the read.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
            log.debug("Handle refreshed: {}.", h);
        }

        Preconditions.checkArgument(offset < h.length(), "Offset %s is beyond the last offset %s of the segment.",
                offset, h.length());
        Preconditions.checkArgument(offset + length <= h.length(), "Offset %s + length %s is beyond the last offset %s of the segment.",
                offset, length, h.length());

        // Read in a loop, from each SegmentChunk, until we can't read anymore.
        // If at any point we encounter a StreamSegmentNotExistsException, fail immediately with StreamSegmentTruncatedException (+inner).
        val chunks = h.chunks();
        int currentIndex = CollectionHelpers.binarySearch(chunks, s -> offset < s.getStartOffset() ? -1 : (offset >= s.getLastOffset() ? 1 : 0));
        assert currentIndex >= 0 : "unable to locate first SegmentChunk index.";

        try {
            int bytesRead = 0;
            while (bytesRead < length && currentIndex < chunks.size()) {
                // Verify if this is a known truncated SegmentChunk; if so, bail out quickly.
                SegmentChunk current = chunks.get(currentIndex);
                checkTruncatedSegment(null, h, current);
                if (current.getLength() == 0) {
                    // Empty SegmentChunk; don't bother trying to read from it.
                    currentIndex++;
                    continue;
                }

                long readOffset = offset + bytesRead - current.getStartOffset();
                int readLength = (int) Math.min(length - bytesRead, current.getLength() - readOffset);
                assert readOffset >= 0 && readLength >= 0 : "negative readOffset or readLength";

                // Read from the actual SegmentChunk into the given buffer.
                try {
                    val sh = this.baseStorage.openRead(current.getName());
                    int count = this.baseStorage.read(sh, readOffset, buffer, bufferOffset + bytesRead, readLength);
                    bytesRead += count;
                    if (readOffset + count >= current.getLength()) {
                        currentIndex++;
                    }
                } catch (StreamSegmentNotExistsException ex) {
                    log.debug("SegmentChunk '{}' does not exist anymore ({}).", current, h);
                    checkTruncatedSegment(ex, h, current);
                }
            }

            LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, bytesRead);
            return bytesRead;
        } catch (StreamSegmentTruncatedException ex) {
            // It's possible that the Segment has been truncated or deleted altogether using another handle. We need to
            // refresh the handle and throw the appropriate exception.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
            if (h.isDeleted()) {
                log.debug("Segment '{}' has been deleted. Cannot read anymore.", h);
                throw new StreamSegmentNotExistsException(handle.getSegmentName(), ex);
            } else {
                throw ex;
            }
        }
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String segmentName) throws StreamSegmentException {
        val handle = (RollingSegmentHandle) openRead(segmentName);
        return StreamSegmentInformation
                .builder()
                .name(handle.getSegmentName())
                .sealed(handle.isSealed())
                .length(handle.length())
                .build();
    }

    @Override
    public boolean exists(String segmentName) {
            // Try to check whether the header file exists.
            return this.baseStorage.exists(StreamSegmentNameUtils.getHeaderSegmentName(segmentName));
    }

    //endregion

    //region SyncStorage Implementation

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
        return create(streamSegmentName, this.defaultRollingPolicy);
    }

    @Override
    public SegmentProperties create(String segmentName, SegmentRollingPolicy rollingPolicy) throws StreamSegmentException {
        Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        String headerName = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        long traceId = LoggerHelpers.traceEnter(log, "create", segmentName, rollingPolicy);

        // First, check if the segment exists but with no header (it might have been created prior to applying
        // RollingStorage to this baseStorage).
        if (this.baseStorage.exists(segmentName)) {
            throw new StreamSegmentExistsException(segmentName);
        }

        // Create the header file, and then serialize the contents to it.
        // If the header file already exists, then it's OK if it's empty (probably a remnant from a previously failed
        // attempt); in that case we ignore it and let the creation proceed.
        SegmentHandle headerHandle = null;
        try {
            try {
                this.baseStorage.create(headerName);
            } catch (StreamSegmentExistsException ex) {
                checkIfEmptyAndNotSealed(ex, headerName);
                log.debug("Empty Segment Header found for '{}'; treating as inexistent.", segmentName);
            }

            headerHandle = this.baseStorage.openWrite(headerName);
            serializeHandle(new RollingSegmentHandle(headerHandle, rollingPolicy, Collections.emptyList()));
        } catch (StreamSegmentExistsException ex) {
            throw ex;
        } catch (Exception ex) {
            if (!Exceptions.mustRethrow(ex) && headerHandle != null) {
                // If we encountered an error while writing the handle file, delete it before returning the exception,
                // otherwise we'll leave behind an empty file.
                try {
                    log.warn("Could not create Header Segment for '{}', rolling back.", segmentName, ex);
                    this.baseStorage.delete(headerHandle);
                } catch (Exception ex2) {
                    ex.addSuppressed(ex2);
                }
            }

            throw ex;
        }

        LoggerHelpers.traceLeave(log, "create", traceId, segmentName);
        return StreamSegmentInformation.builder().name(segmentName).build();
    }

    @Override
    public SegmentHandle openWrite(String segmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", segmentName);
        val handle = openHandle(segmentName, false);

        // Finally, open the Active SegmentChunk for writing.
        SegmentChunk last = handle.lastChunk();
        if (last != null && !last.isSealed()) {
            val activeHandle = this.baseStorage.openWrite(last.getName());
            handle.setActiveChunkHandle(activeHandle);
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId, handle);
        return handle;
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {

        val h = asWritableHandle(handle);
        ensureNotDeleted(h);
        ensureNotSealed(h);
        if (!exists(handle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(handle.getSegmentName());
        }
        ensureOffset(h, offset);
        long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);

        // We run this in a loop because we may have to split the write over multiple SegmentChunks in order to avoid exceeding
        // any SegmentChunk's maximum length.
        int bytesWritten = 0;
        while (bytesWritten < length) {
            if (h.getActiveChunkHandle() == null ) {
                rollover(h);
            }

            SegmentChunk last = h.lastChunk();
            int writeLength = length - bytesWritten;
            assert writeLength > 0 : "non-positive write length";
            long chunkOffset = offset + bytesWritten - last.getStartOffset();
            this.baseStorage.write(h.getActiveChunkHandle(), chunkOffset, data, writeLength);
            last.increaseLength(writeLength);
            bytesWritten += writeLength;
            //Rollover as the next write should happen to another segment..
            rollover(h);

        }

        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, bytesWritten);
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        val h = asWritableHandle(handle);
        ensureNotDeleted(h);
        if (!exists(handle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(handle.getSegmentName());
        }
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        sealActiveChunk(h);
        String sealedName = StreamSegmentNameUtils.getSealedNameFor(h.getSegmentName());
        if (!this.baseStorage.exists(sealedName)) {
            this.baseStorage.create(sealedName);
        }
        h.markSealed();
        log.debug("Sealed Header for '{}'.", h.getSegmentName());
        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }

    @Override
    public void unseal(SegmentHandle handle) {
        throw new UnsupportedOperationException("RollingStorage does not support unseal().");
    }

    @Override
    public void concat(SegmentHandle targetHandle, long targetOffset, String sourceSegment) throws StreamSegmentException {
        val target = asWritableHandle(targetHandle);
        if (!exists(targetHandle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(targetHandle.getSegmentName());
        }
        ensureOffset(target, targetOffset);
        ensureNotDeleted(target);
        ensureNotSealed(target);
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, targetOffset, sourceSegment);

        // We can only use a Segment as a concat source if it is Sealed.
        RollingSegmentHandle source = (RollingSegmentHandle) openWrite(sourceSegment);
        Preconditions.checkState(source.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                sourceSegment, target.getSegmentName());
        if (source.length() == 0) {
            // Source is empty; do not bother with concatenation.
            log.debug("Concat source '{}' is empty. Deleting instead of concatenating.", source);
            delete(source);
            return;
        }

        // We can only use a Segment as a concat source if all of its SegmentChunks exist.
        refreshChunkExistence(source);
        Preconditions.checkState(source.chunks().stream().allMatch(SegmentChunk::exists),
                "Cannot use Segment '%s' as concat source because it is truncated.", source.getSegmentName());

        if (shouldConcatNatively(source, target)) {
            // The Source either does not have a Header or is made up of a single SegmentChunk that can fit entirely into
            // the Target's Active SegmentChunk. Concat it directly without touching the header file; this helps prevent
            // having a lot of very small SegmentChunks around if we end up doing a lot of concatenations.
            log.debug("Concat '{}' into '{}' using native method.", source, target);
            SegmentChunk lastTarget = target.lastChunk();
            if (lastTarget == null || lastTarget.isSealed()) {
                // Make sure the last SegmentChunk of the target is not sealed, otherwise we can't concat into it.
                rollover(target);
            }

            SegmentChunk lastSource = source.lastChunk();
            this.baseStorage.concat(target.getActiveChunkHandle(), target.lastChunk().getLength(), lastSource.getName());
            target.lastChunk().increaseLength(lastSource.getLength());
            if (source.getHeaderHandle() != null) {
                try {
                    this.baseStorage.delete(source.getHeaderHandle());
                } catch (StreamSegmentNotExistsException ex) {
                    // It's ok if it's not there anymore.
                    log.warn("Attempted to delete concat source Header '{}' but it doesn't exist.", source.getHeaderHandle().getSegmentName(), ex);
                }
            }
        } else {
            // Generate new SegmentChunk entries from the SegmentChunks of the Source Segment(but update their start offsets).
            log.debug("Concat '{}' into '{}' using header merge method.", source, target);

            if (target.getHeaderHandle() == null) {
                // We need to concat into a Segment that does not have a Header (yet). Create one before continuing.
                createHeader(target);
            }

            List<SegmentChunk> newSegmentChunks = rebase(source.chunks(), target.length());
            sealActiveChunk(target);
            serializeConcatHeader(target, source);
            // this.baseStorage.concat(target.getHeaderHandle(), target.getHeaderLength(), source.getHeaderHandle().getSegmentName());
            // target.increaseHeaderLength(source.getHeaderLength());
            target.addChunks(newSegmentChunks);

            // After we do a header merge, it's possible that the (new) last chunk may still have space to write to.
            // Unseal it now so that future writes/concats will not unnecessarily create chunks. Note that this will not
            // unseal the segment (even though it's unsealed) - that is determined by the Header file seal status.
            unsealLastChunkIfNecessary(target);
        }

        LoggerHelpers.traceLeave(log, "concat", traceId, target, targetOffset, sourceSegment);
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        val h = asReadableHandle(handle);
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);

        SegmentHandle headerHandle = h.getHeaderHandle();
        if (headerHandle == null) {
            // Directly delete the only SegmentChunk, and bubble up any exceptions if it doesn't exist.
            val subHandle = this.baseStorage.openWrite(h.lastChunk().getName());
            try {
                this.baseStorage.delete(subHandle);
                h.lastChunk().markInexistent();
                h.markDeleted();
            } catch (StreamSegmentNotExistsException ex) {
                h.lastChunk().markInexistent();
                h.markDeleted();
                throw ex;
            }
        } else {
            // We need to seal the whole Segment to prevent anyone else from creating new SegmentChunks while we're deleting
            // them, after which we delete all SegmentChunks and finally the header file.
            if (!h.isSealed()) {
                val writeHandle = h.isReadOnly() ? (RollingSegmentHandle) openWrite(handle.getSegmentName()) : h;
                seal(writeHandle);
            }

            deleteChunks(h, s -> true);
            try {
                this.baseStorage.delete(headerHandle);
                h.markDeleted();
            } catch (StreamSegmentNotExistsException ex) {
                h.markDeleted();
                throw ex;
            }
        }

        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }

    @Override
    public void truncate(SegmentHandle handle, long truncationOffset) throws StreamSegmentException {
        // Delete all SegmentChunks which are entirely before the truncation offset.
        RollingSegmentHandle h = asReadableHandle(handle);
        ensureNotDeleted(h);

        // The only acceptable case where we allow a read-only handle is if the Segment is sealed, since openWrite() will
        // only return a read-only handle in that case.
        Preconditions.checkArgument(h.isSealed() || !h.isReadOnly(), "Can only truncate with a read-only handle if the Segment is Sealed.");
        if (h.getHeaderHandle() == null) {
            // No header means the Segment is made up of a single SegmentChunk. We can't do anything.
            return;
        }

        long traceId = LoggerHelpers.traceEnter(log, "truncate", h, truncationOffset);
        Preconditions.checkArgument(truncationOffset >= 0 && truncationOffset <= h.length(),
                "truncationOffset must be non-negative and at most the length of the Segment.");
        val last = h.lastChunk();
        if (last != null && canTruncate(last, truncationOffset) && !h.isSealed()) {
            // If we were asked to truncate the entire (non-sealed) Segment, then rollover at this point so we can delete
            // all existing data.
            rollover(h);

            // We are free to delete all chunks.
            deleteChunks(h, s -> canTruncate(s, truncationOffset));
        } else {
            // Either we were asked not to truncate the whole segment, or we were, and the Segment is sealed. If the latter,
            // then the Header is also sealed, we could not have done a quick rollover; as such we have no option but to
            // preserve the last chunk so that we can recalculate the length of the Segment if we need it again.
            deleteChunks(h, s -> canTruncate(s, truncationOffset) && s.getLastOffset() < h.length());
        }

        LoggerHelpers.traceLeave(log, "truncate", traceId, h, truncationOffset);
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    //endregion

    //region SegmentChunk Operations

    private void rollover(RollingSegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(handle.getHeaderHandle() != null, "Cannot rollover a Segment with no header.");
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot rollover using a read-only handle.");
        Preconditions.checkArgument(!handle.isSealed(), "Cannot rollover a Sealed Segment.");
        log.debug("Rolling over '{}'.", handle);
        sealActiveChunk(handle);
        createChunk(handle);
    }

    private void sealActiveChunk(RollingSegmentHandle handle) throws StreamSegmentException {
        SegmentHandle activeChunk = handle.getActiveChunkHandle();
        SegmentChunk last = handle.lastChunk();
        if (activeChunk != null && !last.isSealed()) {
            this.baseStorage.seal(activeChunk);
            handle.setActiveChunkHandle(null);
            last.markSealed();
            log.debug("Sealed active SegmentChunk '{}' for '{}'.", activeChunk.getSegmentName(), handle.getSegmentName());
        }
    }

    private void unsealLastChunkIfNecessary(RollingSegmentHandle handle) throws StreamSegmentException {
        SegmentChunk last = handle.lastChunk();
        if (last == null || !last.isSealed()) {
            // Nothing to do.
            return;
        }

        SegmentHandle activeChunk = handle.getActiveChunkHandle();
        boolean needsHandleUpdate = activeChunk == null;
        if (needsHandleUpdate) {
            // We didn't have a pointer to the active chunk's Handle because the chunk was sealed before open-write.
            activeChunk = this.baseStorage.openWrite(last.getName());
        }

        try {
            this.baseStorage.unseal(activeChunk);
        } catch (UnsupportedOperationException e) {
            log.warn("Unable to unseal SegmentChunk '{}' since base storage does not support unsealing.", last);
            return;
        }

        last.markUnsealed();
        if (needsHandleUpdate) {
            activeChunk = this.baseStorage.openWrite(last.getName());
            handle.setActiveChunkHandle(activeChunk);
        }

        log.debug("Unsealed active SegmentChunk '{}' for '{}'.", activeChunk.getSegmentName(), handle.getSegmentName());
    }

    private void createChunk(RollingSegmentHandle handle) throws StreamSegmentException {
        // Create new active SegmentChunk, only after which serialize the handle update and update the handle.
        // We ignore if the SegmentChunk exists and is empty - that's most likely due to a previous failed attempt.
        long segmentLength = handle.length();
        SegmentChunk newSegmentChunk = SegmentChunk.forSegment(handle.getSegmentName(), segmentLength);
        try {
            this.baseStorage.create(newSegmentChunk.getName());
        } catch (StreamSegmentExistsException ex) {
            checkIfEmptyAndNotSealed(ex, newSegmentChunk.getName());
        }

        //serializeNewChunk(handle, newSegmentChunk);
        val activeHandle = this.baseStorage.openWrite(newSegmentChunk.getName());
        handle.addChunk(newSegmentChunk, activeHandle);
        log.debug("Created new SegmentChunk '{}' for '{}'.", newSegmentChunk, handle);
    }

    private void deleteChunks(RollingSegmentHandle handle, Predicate<SegmentChunk> canDelete) throws StreamSegmentException {
        for (SegmentChunk s : handle.chunks()) {
            if (s.exists() && canDelete.test(s)) {
                try {
                    val subHandle = this.baseStorage.openWrite(s.getName());
                    this.baseStorage.delete(subHandle);
                    s.markInexistent();
                    log.debug("Deleted SegmentChunk '{}' for '{}'.", s, handle);
                } catch (StreamSegmentNotExistsException ex) {
                    // Ignore; It's OK if it doesn't exist; just make sure the handle is updated.
                    s.markInexistent();
                }
            }
        }
    }

    private boolean canTruncate(SegmentChunk segmentChunk, long truncationOffset) {
        // We should only truncate those SegmentChunks that are entirely before the truncationOffset. An empty SegmentChunk
        // that starts exactly at the truncationOffset should be spared (this means we truncate the entire Segment), as
        // we need that SegmentChunk to determine the actual length of the Segment.
        return segmentChunk.getStartOffset() < truncationOffset
                && segmentChunk.getLastOffset() <= truncationOffset;
    }

    private void refreshChunkExistence(RollingSegmentHandle handle) {
        // We check all SegmentChunks that we assume exist for actual existence (since once deleted, they can't come back).
        for (SegmentChunk s : handle.chunks()) {
            if (s.exists() && !this.baseStorage.exists(s.getName())) {
                s.markInexistent();
            }
        }
    }

    //endregion

    //region Header Operations

    private void createHeader(RollingSegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(handle.getHeaderHandle() == null, "handle already has a header.");

        // Create a new Header SegmentChunk.
        String headerName = StreamSegmentNameUtils.getHeaderSegmentName(handle.getSegmentName());
        this.baseStorage.create(headerName);
        val headerHandle = this.baseStorage.openWrite(headerName);

        // Create a new Handle and serialize it, after which update the original handle.
        val newHandle = new RollingSegmentHandle(headerHandle, handle.getRollingPolicy(), handle.chunks());
        serializeHandle(newHandle);
        handle.refresh(newHandle);
    }

    private boolean shouldConcatNatively(RollingSegmentHandle source, RollingSegmentHandle target) {
      return false;
    }

    private RollingSegmentHandle openHandle(String segmentName, boolean readOnly) throws StreamSegmentException {
        // Load up the handle from Storage.
        RollingSegmentHandle handle;
        try {
            // Attempt to open using Header.
            val headerInfo = getHeaderInfo(segmentName);
            val headerHandle = readOnly
                    ? this.baseStorage.openRead(headerInfo.getName())
                    : this.baseStorage.openWrite(headerInfo.getName());
            handle = readHeaderAndChunks(headerInfo, headerHandle, segmentName);
        } catch (StreamSegmentNotExistsException ex) {
            // Header does not exist. Attempt to open Segment directly.
            val segmentHandle = readOnly ? this.baseStorage.openRead(segmentName) : this.baseStorage.openWrite(segmentName);
            handle = new RollingSegmentHandle(segmentHandle);
        }

        return handle;
    }

    private SegmentProperties getHeaderInfo(String segmentName) throws StreamSegmentException {
        String headerSegment = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        val headerInfo = this.baseStorage.getStreamSegmentInfo(headerSegment);
        if (headerInfo.getLength() == 0) {
            // We treat empty header files as inexistent segments.
            throw new StreamSegmentNotExistsException(segmentName);
        }

        return headerInfo;
    }

    private RollingSegmentHandle readHeaderAndChunks(SegmentProperties headerInfo, SegmentHandle headerHandle, String segmentName) throws StreamSegmentException {
        byte[] readBuffer = new byte[(int) headerInfo.getLength()];

        this.baseStorage.read(headerHandle, 0, readBuffer, 0, readBuffer.length);
        RollingSegmentHandle handle = HandleSerializer.deserialize(readBuffer, headerHandle);
        String chunkNamePrefix = StreamSegmentNameUtils.getSegmentChunkNamePrefix(segmentName);
        List<String> chunks = this.baseStorage.list(chunkNamePrefix);

        String sealedName = StreamSegmentNameUtils.getSealedNameFor(segmentName);

        chunks.sort((c1, c2) -> {
            if (c1.equals(sealedName)) {
            return -1;
        } else if (c2.equals(sealedName)) {
            return 1;
        } else {
            return Integer.parseInt(c1.substring(c1.lastIndexOf(".") + 1)) -
                    Integer.parseInt(c2.substring(c2.lastIndexOf(".") + 1));
        }
        });

        List<SegmentChunk> chunkList = new ArrayList<>();

        //Add chunks according to offsets ..
        int startOffset = 0;
        if (!chunks.isEmpty()) {
            String first = chunks.get(0);
            startOffset = Integer.parseInt(first.substring(first.lastIndexOf(".") + 1));
        }
        for (String s: chunks) {
            if (StreamSegmentNameUtils.isConcatName(s)) {
                RollingSegmentHandle concatHandle = null;
                try {
                    concatHandle = getConcatSegmentHandleFor(s);
                } catch (StreamSegmentException e) {
                    //TODO: Throw silently
                    e.printStackTrace();
                }
                if (concatHandle != null) {
                    List<SegmentChunk> rebased = rebase(concatHandle.chunks(), startOffset);
                    for (SegmentChunk segmentChunk: rebased) {
                                startOffset += segmentChunk.getLength();
                                segmentChunk.markUnsealed();
                    }

                    chunkList.addAll(rebased);
                }
            } else {
                    SegmentChunk segmentChunk = new SegmentChunk(s, startOffset);
                    segmentChunk.setLength(this.baseStorage.getStreamSegmentInfo(s).getLength());
                    chunkList.add(segmentChunk);
                    startOffset += segmentChunk.getLength();
            }
        }
        handle.addChunks(chunkList);

        if (chunks.contains(StreamSegmentNameUtils.getSealedNameFor(segmentName))) {
            handle.markSealed();
        }
        return handle;
    }

    private RollingSegmentHandle getConcatSegmentHandleFor(String name) throws StreamSegmentException {
        SegmentProperties properties = this.baseStorage.getStreamSegmentInfo(name);
        if (properties.getLength() != 0) {
            SegmentHandle readHandle = this.baseStorage.openRead(name);
            byte[] buffer = new byte[(int) properties.getLength()];
            this.baseStorage.read(readHandle, 0, buffer, 0, buffer.length);
            return this.openHandle(new String(buffer), true);
        } else {
            return null;
        }
    }

    private void serializeHandle(RollingSegmentHandle handle) throws StreamSegmentException {
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        try {
            this.baseStorage.write(handle.getHeaderHandle(), 0, handleData.getReader(), handleData.getLength());
            handle.setHeaderLength(handleData.getLength());
            log.debug("Header for '{}' fully serialized to '{}'.", handle.getSegmentName(), handle.getHeaderHandle().getSegmentName());
        } catch (BadOffsetException ex) {
            // If we get BadOffsetException when writing the Handle, it means it was modified externally.
            throw new StorageNotPrimaryException(handle.getSegmentName(), ex);
        }
    }

    private void serializeConcatHeader(RollingSegmentHandle targetHandle, RollingSegmentHandle sourceHandle) throws StreamSegmentException {
        String concatChunkName = StreamSegmentNameUtils.getconcatName(targetHandle.getSegmentName(), targetHandle.length());
        this.baseStorage.create(concatChunkName);
        SegmentHandle concatHandle = this.baseStorage.openWrite(concatChunkName);
        this.baseStorage.write(concatHandle, 0, new ByteArrayInputStream(sourceHandle.getSegmentName().getBytes()), sourceHandle.getSegmentName().getBytes().length);
    }

    //endregion

    //region Helpers

    private List<SegmentChunk> rebase(List<SegmentChunk> segmentChunks, long newStartOffset) {
        AtomicLong segmentOffset = new AtomicLong(newStartOffset);
        return segmentChunks.stream()
                            .map(s -> s.withNewOffset(segmentOffset.getAndAdd(s.getLength())))
                            .collect(Collectors.toList());
    }

    @SneakyThrows(StreamingException.class)
    private void checkTruncatedSegment(StreamingException ex, RollingSegmentHandle handle, SegmentChunk segmentChunk) {
        if (ex != null && (Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException) || !segmentChunk.exists()) {
            // We ran into a SegmentChunk that does not exist (either marked as such or due to a failed read).
            segmentChunk.markInexistent();
            String message = String.format("Offsets %d-%d have been deleted.", segmentChunk.getStartOffset(), segmentChunk.getLastOffset());
            ex = new StreamSegmentTruncatedException(handle.getSegmentName(), message, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private void checkIfEmptyAndNotSealed(StreamSegmentExistsException ex, String chunkName) throws StreamSegmentException {
        // SegmentChunk exists, check if it's empty and not sealed.
        try {
            val si = this.baseStorage.getStreamSegmentInfo(chunkName);
            if (si.getLength() > 0 || si.isSealed()) {
                throw ex;
            }
        } catch (StreamSegmentNotExistsException notExists) {
            // nothing to do.
        }
    }

    private RollingSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    private RollingSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof RollingSegmentHandle, "handle must be of type RollingSegmentHandle.");
        return (RollingSegmentHandle) handle;
    }

    private void ensureNotDeleted(RollingSegmentHandle handle) throws StreamSegmentNotExistsException {
        if (handle.isDeleted()) {
            throw new StreamSegmentNotExistsException(handle.getSegmentName());
        }
    }

    private void ensureNotSealed(RollingSegmentHandle handle) throws StreamSegmentSealedException {
        if (handle.isSealed()) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }
    }

    private void ensureOffset(RollingSegmentHandle handle, long offset) throws StreamSegmentException {
        if (offset != handle.length()) {
                throw new BadOffsetException(handle.getSegmentName(), handle.length(), offset);
        }
    }

    //endregion
}
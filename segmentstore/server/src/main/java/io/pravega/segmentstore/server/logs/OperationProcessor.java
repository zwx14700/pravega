/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Single-thread Processor for Operations. Queues all incoming entries in a BlockingDrainingQueue, then picks them all
 * at once, generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
class OperationProcessor implements AutoCloseable {
    //region Members

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_READ_AT_ONCE = 1000;
    private static final int MAX_COMMIT_QUEUE_SIZE = 50;

    private final UpdateableContainerMetadata metadata;
    private final MemoryStateUpdater stateUpdater;
    @GuardedBy("stateLock")
    private final OperationMetadataUpdater metadataUpdater;
    private final Object stateLock = new Object();
    private final QueueProcessingState state;
    @GuardedBy("stateLock")
    private final DataFrameBuilder<Operation> dataFrameBuilder;
    @Getter
    private final SegmentStoreMetrics.OperationProcessor metrics;
    //TODO: Ensure that a proper string is mentioned here
    private final String traceObjectId = "OperationsProcessor";

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationProcessor class.
     *
     * @param metadata         The ContainerMetadata for the Container to process operations for.
     * @param stateUpdater     A MemoryStateUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog   The DataFrameLog to write DataFrames to.
     * @param checkpointPolicy The Checkpoint Policy for Metadata.
     * @param executor         An Executor to use for async operations.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationProcessor(UpdateableContainerMetadata metadata, MemoryStateUpdater stateUpdater, DurableDataLog durableDataLog, MetadataCheckpointPolicy checkpointPolicy, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        this.metadata = metadata;
        this.stateUpdater = Preconditions.checkNotNull(stateUpdater, "stateUpdater");
        this.metadataUpdater = new OperationMetadataUpdater(this.metadata);
        this.state = new QueueProcessingState(checkpointPolicy);
        val args = new DataFrameBuilder.Args(this.state::frameSealed, this.state::commit, this.state::fail, executor);
        this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        this.metrics = new SegmentStoreMetrics.OperationProcessor(this.metadata.getContainerId());
    }

    //endregion

    //region Operations Processing

    /**
     * Processes the given Operation. This method returns when the given Operation has been scheduled.
     *
     * @param operation The Operation to process.
     * @return A CompletableFuture that, when completed, will indicate the Operation has finished processing. If the
     * Operation completed successfully, the Future will contain the Sequence Number of the Operation. If the Operation
     * failed, it will contain the exception that caused the failure.
     * @throws IllegalContainerStateException If the OperationProcessor is not running.
     */
    public CompletableFuture<Void> process(Operation operation) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        //TODO: use closed state to check whether this is still running ..
        log.debug("{}: process {}.", this.traceObjectId, operation);
        try {
            this.processOperation(new CompletableOperation(operation, result));
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    //endregion

    //region Operation Processing

    /**
     * Processes a single operation.
     * Steps:
     * <ol>
     * <li> Pre-processes operation (in MetadataUpdater).
     * <li> Assigns Sequence Number.
     * <li> Appends to DataFrameBuilder.
     * <li> Accepts operation in MetadataUpdater.
     * </ol>
     *
     * @param operation        The operation to process.
     * @throws Exception If an exception occurred while processing this operation. Depending on the type of the exception,
     * this could be due to the operation itself being invalid, or because we are unable to process any more operations.
     */
    private void processOperation(CompletableOperation operation) throws Exception {
        Preconditions.checkState(!operation.isDone(), "The Operation has already been processed.");

        Operation entry = operation.getOperation();
        if (!entry.canSerialize()) {
            // This operation cannot be serialized, so don't bother doing anything with it.
            return;
        }

        synchronized (this.stateLock) {
            this.state.addPending(operation);
            // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater
            // has all the knowledge for that task.
            this.metadataUpdater.preProcessOperation(entry);

            // Entry is ready to be serialized; assign a sequence number.
            entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());
            this.dataFrameBuilder.append(entry);
            this.metadataUpdater.acceptOperation(entry);
            this.dataFrameBuilder.flush();
        }

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, entry);
    }

    /**
     * Determines whether the given Throwable is a fatal exception from which we cannot recover.
     */
    private static boolean isFatalException(Throwable ex) {
        return ex instanceof DataCorruptionException
                || ex instanceof DataLogWriterNotPrimaryException
                || ex instanceof ObjectClosedException;
    }

    @Override
    public void close() throws Exception {

    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the OperationProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     * Note: this class shares state with OperationProcessor, as it accesses many of its private fields and uses its stateLock
     * for synchronization. Care should be taken if it is refactored out of here.
     */
    @ThreadSafe
    private class QueueProcessingState {
        @GuardedBy("stateLock")
        private ArrayList<CompletableOperation> nextFrameOperations;
        @GuardedBy("stateLock")
        private int pendingOperationCount;
        private final MetadataCheckpointPolicy checkpointPolicy;
        @GuardedBy("stateLock")
        private final ArrayDeque<DataFrameBuilder.CommitArgs> metadataTransactions;
        @GuardedBy("stateLock")
        private long highestCommittedDataFrame;

        private QueueProcessingState(MetadataCheckpointPolicy checkpointPolicy) {
            this.checkpointPolicy = Preconditions.checkNotNull(checkpointPolicy, "checkpointPolicy");
            this.nextFrameOperations = new ArrayList<>();
            this.metadataTransactions = new ArrayDeque<>();
            this.highestCommittedDataFrame = -1;
            this.pendingOperationCount = 0;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        void addPending(CompletableOperation operation) {
            boolean autoComplete = false;
            synchronized (stateLock) {
                if (this.nextFrameOperations.isEmpty() && !operation.getOperation().canSerialize()) {
                    autoComplete = true;
                } else {
                    this.nextFrameOperations.add(operation);
                    this.pendingOperationCount++;
                }
            }

            if (autoComplete) {
                operation.complete();
            }
        }

        /**
         * Gets a value indicating the number of pending operations
         *
         * @return The count.
         */
        int getPendingCount() {
            synchronized (stateLock) {
                return this.pendingOperationCount;
            }
        }

        /**
         * Callback for when a DataFrame has been Sealed and is ready to be written to the DurableDataLog.
         * Seals the current metadata UpdateTransaction and maps it to the given CommitArgs. This UpdateTransaction
         * marks a point in the OperationMetadataUpdater that corresponds to the state of the Log at the end of the
         * DataFrame represented by the given commitArgs.
         *
         * @param commitArgs The CommitArgs to create a checkpoint for.
         */
        void frameSealed(DataFrameBuilder.CommitArgs commitArgs) {
            synchronized (stateLock) {
                commitArgs.setMetadataTransactionId(OperationProcessor.this.metadataUpdater.sealTransaction());
                commitArgs.setOperations(Collections.unmodifiableList(this.nextFrameOperations));
                this.nextFrameOperations = new ArrayList<>();
                this.metadataTransactions.addLast(commitArgs);
            }
        }

        /**
         * Callback for when a DataFrame has been successfully written to the DurableDataLog.
         * Commits all pending Metadata changes, assigns a TruncationMarker mapped to the given commitArgs and
         * acknowledges the pending operations up to the given commitArgs.
         *
         * It is important to note that this call is inclusive of all calls with arguments prior to it. It will
         * automatically complete all UpdateTransactions (and their corresponding operations) for all commitArgs that are
         * still registered but have a key smaller than the one in the given argument.
         *
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        void commit(DataFrameBuilder.CommitArgs commitArgs) {
            assert commitArgs.getMetadataTransactionId() >= 0 : "DataFrameBuilder.CommitArgs does not have a key set";
            log.debug("{}: CommitSuccess ({}).", traceObjectId, commitArgs);
            Timer timer = new Timer();

            List<List<CompletableOperation>> toAck = null;
            try {
                // Record the end of a frame in the DurableDataLog directly into the base metadata. No need for locking here,
                // as the metadata has its own.
                OperationProcessor.this.metadata.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getLogAddress());
                final long addressSequence = commitArgs.getLogAddress().getSequence();

                synchronized (stateLock) {
                    if (addressSequence <= this.highestCommittedDataFrame) {
                        // Ack came out of order (we already processed one with a higher SeqNo).
                        log.debug("{}: CommitRejected ({}, HighestCommittedDataFrame = {}).", traceObjectId, commitArgs, this.highestCommittedDataFrame);
                        return;
                    }

                    // Collect operations to commit.
                    Timer memoryCommitTimer = new Timer();
                    toAck = collectCompletionCandidates(commitArgs);

                    // Commit metadata updates.
                    int updateTxnCommitCount = OperationProcessor.this.metadataUpdater.commit(commitArgs.getMetadataTransactionId());

                    // Commit operations to memory. Note that this will block synchronously if the Commit Queue is full (until it clears up).
                    toAck.forEach((op) -> {
                        op.forEach((single) -> {
                            try {
                                stateUpdater.process(single.getOperation());
                                single.complete();
                            } catch (DataCorruptionException e) {
                                e.printStackTrace();
                                single.fail(e);
                            }
                        });
                    });

                    this.highestCommittedDataFrame = addressSequence;
                    metrics.memoryCommit(updateTxnCommitCount, memoryCommitTimer.getElapsed());
                }
            } finally {
                if (toAck != null) {
                    toAck.stream().flatMap(Collection::stream).forEach(CompletableOperation::complete);
                    metrics.operationsCompleted(toAck, timer.getElapsed());
                }
                autoCompleteIfNeeded();
                this.checkpointPolicy.recordCommit(commitArgs.getDataFrameLength());
            }
        }

        /**
         * Callback for when a DataFrame has failed to be written to the DurableDataLog.
         * Rolls back pending Metadata changes that are mapped to the given commitArgs (and after) and fails all pending
         * operations that are affected.
         *
         * It is important to note that this call is inclusive of all calls with arguments after it. It will automatically
         * complete all UpdateTransactions (and their corresponding operations) for all commitArgs that are registered but
         * have a key larger than the one in the given argument.
         *
         * @param ex The cause of the failure. The operations will be failed with this as a cause.
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        void fail(Throwable ex, DataFrameBuilder.CommitArgs commitArgs) {
            List<CompletableOperation> toFail = null;
            try {
                synchronized (stateLock) {
                    toFail = collectFailureCandidates(commitArgs);
                    this.pendingOperationCount -= toFail.size();
                }
            } finally {
                if (toFail != null) {
                    toFail.forEach(o -> failOperation(o, ex));
                    metrics.operationsFailed(toFail);
                }

                autoCompleteIfNeeded();
            }

            // All exceptions are final. If we cannot write to DurableDataLog, the safest way out is to shut down and
            // perform a new recovery that will detect any possible data loss or corruption.
            // TODO: Find out what this call is doing
            // Callbacks.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
        }

        /**
         * Fails the given Operation either with the given failure cause, or with the general stop exception.
         *
         * @param operation    The CompletableOperation to fail.
         * @param failureCause The original failure cause. The operation will be failed with this exception, unless
         *                     the general stopException is set, in which case that takes precedence.
         */
        void failOperation(CompletableOperation operation, Throwable failureCause) {
            operation.fail(failureCause);
        }

        /**
         * Collects all Operations that have been successfully committed to DurableDataLog and removes the associated
         * Metadata Update Transactions for those commits. The operations themselves are not completed (since we are
         * holding a lock) and the Metadata is not updated while executing this method.
         *
         * @param commitArgs The CommitArgs that points to the DataFrame which successfully committed.
         * @return An ordered List of ordered Lists of Operations that have been committed.
         */
        @GuardedBy("stateLock")
        private List<List<CompletableOperation>> collectCompletionCandidates(DataFrameBuilder.CommitArgs commitArgs) {
            List<List<CompletableOperation>> toAck = new ArrayList<>();
            long transactionId = commitArgs.getMetadataTransactionId();
            boolean checkpointExists = false;
            while (!this.metadataTransactions.isEmpty() && this.metadataTransactions.peekFirst().getMetadataTransactionId() <= transactionId) {
                DataFrameBuilder.CommitArgs t = this.metadataTransactions.pollFirst();
                checkpointExists |= t.getMetadataTransactionId() == transactionId;
                if (t.getOperations().size() > 0) {
                    toAck.add(t.getOperations());
                    this.pendingOperationCount -= t.getOperations().size();
                }
            }

            assert checkpointExists : "No Metadata UpdateTransaction found for " + commitArgs;
            return toAck;
        }

        /**
         * Rolls back any metadata that is affected by a failure for the given commit args and collects all pending
         * CompletableOperations that are affected. While the metadata is rolled back, the operations themselves
         * are not failed (since this method executes while holding the lock).
         *
         * @param commitArgs The CommitArgs that points to the DataFrame which failed to commit.
         * @return A List where the failure candidates will be collected.
         */
        @GuardedBy("stateLock")
        private List<CompletableOperation> collectFailureCandidates(DataFrameBuilder.CommitArgs commitArgs) {
            // Discard all updates to the metadata.
            List<CompletableOperation> candidates = new ArrayList<>();
            if (commitArgs != null) {
                // Rollback all changes to the metadata from this commit on, and fail all involved operations.
                OperationProcessor.this.metadataUpdater.rollback(commitArgs.getMetadataTransactionId());
                while (!this.metadataTransactions.isEmpty() && this.metadataTransactions.peekLast().getMetadataTransactionId() >= commitArgs.getMetadataTransactionId()) {
                    // Fail all operations in this particular commit.
                    DataFrameBuilder.CommitArgs t = this.metadataTransactions.pollLast();
                    candidates.addAll(t.getOperations());
                }
            } else {
                // Rollback all changes to the metadata and fail all outstanding commits.
                this.metadataTransactions.forEach(t -> candidates.addAll(t.getOperations()));
                this.metadataTransactions.clear();
                OperationProcessor.this.metadataUpdater.rollback(0);
            }

            candidates.addAll(this.nextFrameOperations);
            this.nextFrameOperations.clear();
            return candidates;
        }

        /**
         * Auto-completes any non-serialization operations at the beginning of the Pending Operations queue. Due to their
         * nature, these operations are at risk of never being completed, and, if there are no more pending operations
         * before that, they can be completed without further delay.
         */
        private void autoCompleteIfNeeded() {
            Collection<CompletableOperation> toComplete = null;
            synchronized (stateLock) {
                for (CompletableOperation o : this.nextFrameOperations) {
                    if (o.getOperation().canSerialize()) {
                        break;
                    }

                    if (toComplete == null) {
                        toComplete = new ArrayList<>();
                    }

                    toComplete.add(o);
                }
            }

            if (toComplete != null) {
                toComplete.forEach(CompletableOperation::complete);
            }
        }
    }

    //endregion
}

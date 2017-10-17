/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Utility Service that advances the watermark on idle segments on a periodic basis.
 */
@Slf4j
@ThreadSafe
class WatermarkAdvancer extends AbstractThreadPoolService {
    //region Private

    private final ContainerConfig config;
    private final StreamSegmentContainer container;
    private final CancellationToken stopToken;
    private final Duration delayDuration;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WatermarkAdvancer class.
     *
     * @param config          Container Configuration to use.
     * @param container       The segment container.
     * @param traceObjectId   An identifier to use for logging purposes. This will be included at the beginning of all
     *                        log calls initiated by this Service.
     * @param executor        The Executor to use for async callbacks and operations.
     */
    WatermarkAdvancer(ContainerConfig config, StreamSegmentContainer container, ScheduledExecutorService executor, String traceObjectId) {
        super(traceObjectId, executor);
        Preconditions.checkNotNull(container, "container");

        this.config = config;
        this.container = container;
        this.stopToken = new CancellationToken();
        this.delayDuration = config.getWatermarkPollingPeriod();
    }

    //endregion

    //region AbstractThreadPooledService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return Duration.ofSeconds(30);
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return FutureHelpers.loop(
                () -> !this.stopToken.isCancellationRequested(),
                () -> delay().thenRun(container::advanceWatermarks),
                this.executor);
    }

    @Override
    protected void doStop() {
        this.stopToken.requestCancellation();
        super.doStop();
    }

    //endregion

    private CompletableFuture<Void> delay() {
        val result = FutureHelpers.delayedFuture(this.delayDuration, this.executor);
        this.stopToken.register(result);
        return result;
    }
}
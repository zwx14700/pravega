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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderedExecutor {
    private final Map<Object, CompletableFuture<?>> currentFutures;
    @Getter
    private final ScheduledExecutorService executor;
    private int count = 0;

    public OrderedExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        currentFutures = new HashMap<>();
    }

    /**
     * Schedules a one time action to execute with an ordering guarantee on the key.
     * @param orderingKey   key on which the tasks are scheduled
     * @param r             the actual task.
     */
    public Future<?> submitOrdered(Object orderingKey, Runnable r) {
        synchronized (this) {
            log.trace("Scheduling op {}", ++count);
        }

        executor.submit(() -> {
            r.run();
            synchronized (this) {
                log.trace("Done with op {}", --count);

            }
        });
        return null;
        /*
        synchronized (currentFutures) {
            CompletableFuture<?> newFuture;

            if (currentFutures.containsKey(orderingKey)) {
                newFuture = currentFutures.get(orderingKey).thenRun(r);
            } else {
                newFuture = new CompletableFuture<>();
                        executor.submit(() -> {
                            r.run();
                            ((CompletableFuture<?>) newFuture).complete(null);
                            if (currentFutures.get(orderingKey) == newFuture) {
                                currentFutures.remove(orderingKey);
                            }
                        });
            }
            currentFutures.put(orderingKey, newFuture);
            return newFuture;
            */
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.common;

import lombok.ToString;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock based on a counter that increments when the current time is requested.
 *
 * The initial value of the counter is {@code 42} to easily distinguish the value as a time.
 */
@ToString
public class CounterClock extends Clock implements Serializable {

    private static final long serialVersionUID = 0L;

    private final ZoneId zone;
    private final AtomicLong counter;

    public CounterClock() {
        this.zone = ZoneId.systemDefault();
        this.counter = new AtomicLong(42L);
    }

    public CounterClock(ZoneId zone, long initialValue) {
        this.zone = zone;
        this.counter = new AtomicLong(initialValue);
    }

    public AtomicLong getCounter() {
        return counter;
    }

    // region Clock Implementation

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new CounterClock(zone, counter.get());
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(counter.getAndIncrement());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CounterClock that = (CounterClock) o;
        return Objects.equals(zone, that.zone) &&
                Objects.equals(counter.get(), that.counter.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zone, counter.get());
    }

    // endregion
}

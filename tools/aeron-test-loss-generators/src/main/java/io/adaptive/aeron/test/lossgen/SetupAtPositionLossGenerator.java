/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Vendored from aeron-test-support for use in Aeron.NET integration tests.
 *
 * Drops incoming SETUP frames whose (streamId, initialTermId, activeTermId, termOffset)
 * tuple matches a target. DATA frames always pass.
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.driver.ext.LossGenerator;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

public final class SetupAtPositionLossGenerator implements LossGenerator
{
    private static final VarHandle ENABLED_VH;

    static
    {
        try
        {
            ENABLED_VH = MethodHandles.lookup()
                .findVarHandle(SetupAtPositionLossGenerator.class, "enabled", boolean.class);
        }
        catch (final NoSuchFieldException | IllegalAccessException e)
        {
            throw new Error(e);
        }
    }

    private final SetupFlyweight setupFlyweight = new SetupFlyweight();
    private int streamId;
    private int initialTermId;
    private int activeTermId;
    private int termOffset;
    private volatile boolean enabled;
    private final AtomicInteger setupsDropped = new AtomicInteger();

    public void setTarget(
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset)
    {
        this.streamId = streamId;
        this.initialTermId = initialTermId;
        this.activeTermId = activeTermId;
        this.termOffset = termOffset;
    }

    public void enable()
    {
        ENABLED_VH.setRelease(this, true);
    }

    public void disable()
    {
        ENABLED_VH.setRelease(this, false);
    }

    public int setupsDropped()
    {
        return setupsDropped.get();
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        if (!(boolean)ENABLED_VH.getAcquire(this))
        {
            return false;
        }

        if (length < SetupFlyweight.HEADER_LENGTH)
        {
            return false;
        }

        final int type = buffer.getShort(HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        if (HeaderFlyweight.HDR_TYPE_SETUP != type)
        {
            return false;
        }

        setupFlyweight.wrap(buffer, 0, length);
        if (setupFlyweight.streamId() != this.streamId ||
            setupFlyweight.initialTermId() != this.initialTermId ||
            setupFlyweight.activeTermId() != this.activeTermId ||
            setupFlyweight.termOffset() != this.termOffset)
        {
            return false;
        }

        setupsDropped.incrementAndGet();
        return true;
    }

    public boolean shouldDropFrame(
        final InetSocketAddress address,
        final UnsafeBuffer buffer,
        final int streamId,
        final int sessionId,
        final int termId,
        final int termOffset,
        final int length)
    {
        return false;
    }
}

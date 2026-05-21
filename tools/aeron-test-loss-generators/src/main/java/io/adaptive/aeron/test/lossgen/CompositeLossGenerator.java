/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Combines several LossGenerator instances so the driver's debug channel endpoints
 * (which accept exactly one LossGenerator per direction) can consult all registered
 * generators in OR fashion.
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.driver.ext.LossGenerator;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

public final class CompositeLossGenerator implements LossGenerator
{
    private final LossGenerator[] generators;

    public CompositeLossGenerator(final LossGenerator... generators)
    {
        this.generators = generators;
    }

    public CompositeLossGenerator(final String tag, final LossGenerator... generators)
    {
        // tag is descriptive only; kept for future logging without changing the call sites.
        this.generators = generators;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        for (final LossGenerator g : generators)
        {
            if (g.shouldDropFrame(address, buffer, length))
            {
                return true;
            }
        }
        return false;
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
        for (final LossGenerator g : generators)
        {
            if (g.shouldDropFrame(address, buffer, streamId, sessionId, termId, termOffset, length))
            {
                return true;
            }
        }
        return false;
    }
}

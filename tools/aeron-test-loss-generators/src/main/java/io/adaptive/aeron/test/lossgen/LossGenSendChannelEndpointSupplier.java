/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Wraps every outgoing channel endpoint in DebugSendChannelEndpoint backed by the
 * shared LossGenRegistry singletons. Acts as a no-op when no generator is enabled.
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.AtomicCounter;

public final class LossGenSendChannelEndpointSupplier implements SendChannelEndpointSupplier
{
    public SendChannelEndpoint newInstance(
        final UdpChannel udpChannel,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        final LossGenerator dataLossGenerator = new CompositeLossGenerator(
            "send-data:" + udpChannel.originalUriString(),
            LossGenRegistry.frameData(),
            LossGenRegistry.streamIdFrameData(),
            LossGenRegistry.streamId(),
            LossGenRegistry.dataInRange());
        final LossGenerator controlLossGenerator = new CompositeLossGenerator(
            "send-ctrl:" + udpChannel.originalUriString(),
            LossGenRegistry.frameData(),
            LossGenRegistry.setupAtPosition());
        return new DebugSendChannelEndpoint(
            udpChannel, statusIndicator, context, dataLossGenerator, controlLossGenerator);
    }
}

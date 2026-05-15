/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Wraps every incoming channel endpoint in DebugReceiveChannelEndpoint backed by the
 * shared LossGenRegistry singletons. Acts as a no-op when no generator is enabled.
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.AtomicCounter;

public final class LossGenReceiveChannelEndpointSupplier implements ReceiveChannelEndpointSupplier
{
    public ReceiveChannelEndpoint newInstance(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        // DebugReceiveChannelEndpoint routes incoming SETUPs (and RTT responses) through the
        // *data* loss generator — not the control one, despite the names. Control here is the
        // OUTGOING NAK/RTT path. So SetupAtPosition must live with the inbound DATA filters.
        final LossGenerator dataLossGenerator = new CompositeLossGenerator(
            "recv-data:" + udpChannel.originalUriString(),
            LossGenRegistry.frameData(),
            LossGenRegistry.streamIdFrameData(),
            LossGenRegistry.streamId(),
            LossGenRegistry.dataInRange(),
            LossGenRegistry.setupAtPosition());
        final LossGenerator controlLossGenerator = new CompositeLossGenerator(
            "send-ctrl:" + udpChannel.originalUriString(),
            LossGenRegistry.frameData());
        return new DebugReceiveChannelEndpoint(
            udpChannel, dispatcher, statusIndicator, context, dataLossGenerator, controlLossGenerator);
    }
}

/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Singleton registry of loss generator instances installed in this JVM. The
 * driver's debug channel endpoints pull the singletons from here at construction;
 * the LossGenControlAgent updates their state via commands received from the .NET
 * test process.
 */
package io.adaptive.aeron.test.lossgen;

public final class LossGenRegistry
{
    public static final int CONTROL_STREAM_ID = 10001;
    public static final String CONTROL_CHANNEL = "aeron:ipc?session-id=99999";

    private static final FrameDataLossGenerator FRAME_DATA = new FrameDataLossGenerator();
    private static final StreamIdLossGenerator STREAM_ID = new StreamIdLossGenerator();
    private static final StreamIdFrameDataLossGenerator STREAM_ID_FRAME_DATA = new StreamIdFrameDataLossGenerator();
    private static final DataInRangeLossGenerator DATA_IN_RANGE = new DataInRangeLossGenerator();
    private static final SetupAtPositionLossGenerator SETUP_AT_POSITION = new SetupAtPositionLossGenerator();

    private LossGenRegistry()
    {
    }

    public static FrameDataLossGenerator frameData()
    {
        return FRAME_DATA;
    }

    public static StreamIdLossGenerator streamId()
    {
        return STREAM_ID;
    }

    public static StreamIdFrameDataLossGenerator streamIdFrameData()
    {
        return STREAM_ID_FRAME_DATA;
    }

    public static DataInRangeLossGenerator dataInRange()
    {
        return DATA_IN_RANGE;
    }

    public static SetupAtPositionLossGenerator setupAtPosition()
    {
        return SETUP_AT_POSITION;
    }
}

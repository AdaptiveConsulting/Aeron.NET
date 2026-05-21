/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Subscribes to LossGenRegistry.CONTROL_CHANNEL on a dedicated thread and dispatches
 * commands to the registered loss generators. Wire format is little-endian binary:
 *
 *   byte 0:    command opcode (see CMD_* constants)
 *   bytes 1+:  command-specific arguments
 *
 * Predicates are not serialised; they are referenced by enum (PRED_*) and reconstructed
 * server-side. The Predicate<byte[]> for FrameData / StreamIdFrameData generators is
 * encoded as a small constant followed by the predicate's args.
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public final class LossGenControlAgent implements Runnable
{
    public static final byte CMD_FRAME_DATA_ENABLE = 0x01;
    public static final byte CMD_FRAME_DATA_DISABLE = 0x02;
    public static final byte CMD_STREAM_ID_ENABLE = 0x03;
    public static final byte CMD_STREAM_ID_DISABLE = 0x04;
    public static final byte CMD_STREAM_ID_FRAME_DATA_ENABLE = 0x05;
    public static final byte CMD_STREAM_ID_FRAME_DATA_DISABLE = 0x06;
    public static final byte CMD_DATA_IN_RANGE_SET_TARGET = 0x07;
    public static final byte CMD_DATA_IN_RANGE_ENABLE = 0x08;
    public static final byte CMD_DATA_IN_RANGE_DISABLE = 0x09;
    public static final byte CMD_SETUP_AT_POSITION_SET_TARGET = 0x0A;
    public static final byte CMD_SETUP_AT_POSITION_ENABLE = 0x0B;
    public static final byte CMD_SETUP_AT_POSITION_DISABLE = 0x0C;

    public static final byte PRED_ALWAYS_TRUE = 0x00;
    public static final byte PRED_RANDOM_FRACTION = 0x01;
    // Drops everything starting from the first frame whose payload (after the DATA header)
    // matches a target byte sequence — used to test that PS notices a half-fragmented message
    // when the second half is dropped and falls back to replay.
    public static final byte PRED_PAYLOAD_EQUALS_STICKY = 0x02;

    private final Aeron aeron;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final FragmentHandler handler;

    public LossGenControlAgent(final Aeron aeron)
    {
        this.aeron = aeron;
        this.handler = new FragmentAssembler(this::onMessage);
    }

    public void stop()
    {
        running.set(false);
    }

    public void run()
    {
        try (Subscription subscription = aeron.addSubscription(
            LossGenRegistry.CONTROL_CHANNEL, LossGenRegistry.CONTROL_STREAM_ID))
        {
            while (running.get())
            {
                if (subscription.poll(handler, 10) == 0)
                {
                    try
                    {
                        Thread.sleep(1);
                    }
                    catch (final InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final Object header)
    {
        if (length < 1)
        {
            return;
        }
        final byte cmd = buffer.getByte(offset);
        switch (cmd)
        {
            case CMD_FRAME_DATA_ENABLE:
                LossGenRegistry.frameData().enable(decodePredicate(buffer, offset + 1));
                break;
            case CMD_FRAME_DATA_DISABLE:
                LossGenRegistry.frameData().disable();
                break;
            case CMD_STREAM_ID_ENABLE:
                LossGenRegistry.streamId().enable(buffer.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN));
                break;
            case CMD_STREAM_ID_DISABLE:
                LossGenRegistry.streamId().disable();
                break;
            case CMD_STREAM_ID_FRAME_DATA_ENABLE:
                LossGenRegistry.streamIdFrameData().enable(
                    buffer.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN),
                    decodePredicate(buffer, offset + 5));
                break;
            case CMD_STREAM_ID_FRAME_DATA_DISABLE:
                LossGenRegistry.streamIdFrameData().disable();
                break;
            case CMD_DATA_IN_RANGE_SET_TARGET:
                LossGenRegistry.dataInRange().setTarget(
                    buffer.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 5, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 9, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 13, ByteOrder.LITTLE_ENDIAN));
                break;
            case CMD_DATA_IN_RANGE_ENABLE:
                LossGenRegistry.dataInRange().enable();
                break;
            case CMD_DATA_IN_RANGE_DISABLE:
                LossGenRegistry.dataInRange().disable();
                break;
            case CMD_SETUP_AT_POSITION_SET_TARGET:
                LossGenRegistry.setupAtPosition().setTarget(
                    buffer.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 5, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 9, ByteOrder.LITTLE_ENDIAN),
                    buffer.getInt(offset + 13, ByteOrder.LITTLE_ENDIAN));
                break;
            case CMD_SETUP_AT_POSITION_ENABLE:
                LossGenRegistry.setupAtPosition().enable();
                break;
            case CMD_SETUP_AT_POSITION_DISABLE:
                LossGenRegistry.setupAtPosition().disable();
                break;
            default:
                System.err.println("LossGenControlAgent: unknown cmd " + cmd);
                break;
        }
    }

    private static Predicate<byte[]> decodePredicate(final DirectBuffer buffer, final int offset)
    {
        final byte predType = buffer.getByte(offset);
        switch (predType)
        {
            case PRED_ALWAYS_TRUE:
                return bytes -> true;
            case PRED_RANDOM_FRACTION:
            {
                final double fraction = buffer.getDouble(offset + 1, ByteOrder.LITTLE_ENDIAN);
                return bytes -> ThreadLocalRandom.current().nextDouble() < fraction;
            }
            case PRED_PAYLOAD_EQUALS_STICKY:
            {
                final int matchLen = buffer.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN);
                final byte[] match = new byte[matchLen];
                buffer.getBytes(offset + 5, match);
                final java.util.concurrent.atomic.AtomicBoolean sticky =
                    new java.util.concurrent.atomic.AtomicBoolean(false);
                return bytes ->
                {
                    if (sticky.get())
                    {
                        return true;
                    }
                    final int headerLen = io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
                    if (bytes.length - headerLen != matchLen)
                    {
                        return false;
                    }
                    for (int i = 0; i < matchLen; i++)
                    {
                        if (bytes[headerLen + i] != match[i])
                        {
                            return false;
                        }
                    }
                    sticky.set(true);
                    return true;
                };
            }
            default:
                throw new IllegalArgumentException("unknown predicate type " + predType);
        }
    }
}

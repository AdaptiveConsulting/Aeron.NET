/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Entry point that launches the Aeron media driver and a LossGenControlAgent in the
 * same JVM. The control agent connects an Aeron client to the just-started driver
 * and processes commands published from the .NET test process on
 * LossGenRegistry.CONTROL_CHANNEL.
 *
 * Drop-in replacement for `io.aeron.driver.MediaDriver` as a process entry point.
 * The send/receive channel endpoint suppliers in this jar must already be installed
 * via system properties (`aeron.driver.send.channel.endpoint.supplier=...` and
 * `aeron.driver.receive.channel.endpoint.supplier=...`).
 */
package io.adaptive.aeron.test.lossgen;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;

public final class LossGenMediaDriver
{
    public static void main(final String[] args) throws Exception
    {
        try (MediaDriver driver = MediaDriver.launch();
             Aeron aeron = Aeron.connect(new Aeron.Context()
                 .aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final LossGenControlAgent agent = new LossGenControlAgent(aeron);
            final Thread agentThread = new Thread(agent, "loss-gen-control-agent");
            agentThread.setDaemon(true);
            agentThread.start();

            new ShutdownSignalBarrier().await();
            agent.stop();
        }
    }
}

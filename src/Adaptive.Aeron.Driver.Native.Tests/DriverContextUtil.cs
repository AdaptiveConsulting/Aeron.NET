using System;
using System.IO;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    public static class DriverContextUtil
    {
        public static AeronDriver.DriverContext CreateDriverCtx()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dir = Path.Combine(baseDir, "aeron-" + Guid.NewGuid().ToString("N"));

            var ctx = new AeronDriver.DriverContext()
                .AeronDirectoryName(dir)
                // .DebugTimeoutMs(60 * 1000)
                .ThreadingMode(AeronThreadingModeEnum.AeronThreadingModeShared)
                .SharedIdleStrategy(DriverIdleStrategy.SLEEPING)
                .TermBufferLength(128 * 1024)
                .DirDeleteOnStart(true)
                .DirDeleteOnShutdown(true)
                .PrintConfigurationOnStart(false)
                .LoggerInfo(s => Console.WriteLine($"INFO: {s}"))
                .LoggerWarning(s => Console.WriteLine($"WARN: {s}"))
                .LoggerError(s => Console.Error.WriteLine($"ERROR: {s}"));

            return ctx;
        }
    }
}
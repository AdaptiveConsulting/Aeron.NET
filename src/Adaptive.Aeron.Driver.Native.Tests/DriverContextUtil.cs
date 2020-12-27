using System;
using System.IO;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    public static class DriverContextUtil
    {
        public static AeronDriver.DriverContext CreateDriverCtx(bool isServer = false)
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dir = Path.Combine(baseDir, "mediadrivers", (isServer ? "server_" : "") + Guid.NewGuid().ToString("N"));

            var ctx = new Aeron.Context()
                .AeronDirectoryName(dir)
                .DriverContext()
                .DebugTimeoutMs(60 * 1000)
                .ThreadingMode(AeronThreadingModeEnum.AeronThreadingModeShared)
                .SharedIdleStrategy(DriverIdleStrategy.SLEEPING)
                .TermBufferLength(128 * 1024)
                .DirDeleteOnStart(true)
                .DirDeleteOnShutdown(true)
                .PrintConfigurationOnStart(true)
                .LoggerInfo(s => Console.WriteLine($"INFO: {s}"))
                .LoggerWarning(s => Console.WriteLine($"WARN: {s}"))
                .LoggerError(s => Console.Error.WriteLine($"ERROR: {s}"));

            return ctx;
        }
    }
}

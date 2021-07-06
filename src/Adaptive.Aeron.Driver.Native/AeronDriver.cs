using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using static Adaptive.Aeron.Driver.Native.Interop;

namespace Adaptive.Aeron.Driver.Native
{
    public partial class AeronDriver
    {
        private readonly struct NativeDriver
        {
            public readonly IntPtr Driver;
            public readonly IntPtr Ctx;

            public NativeDriver(IntPtr driver, IntPtr ctx)
            {
                Driver = driver;
                Ctx = ctx;
            }
        }

        private readonly DriverContext _dCtx;
        private readonly NativeDriver _native;

        private volatile int _isDriverRunning;
        private readonly EventHandler _processExitHandler;

        private AeronDriver(DriverContext driverContext, NativeDriver native)
        {
            _dCtx = driverContext;
            _native = native;
            _isDriverRunning = 1;

            _processExitHandler = OnCurrentDomainOnProcessExit;
            AppDomain.CurrentDomain.ProcessExit += _processExitHandler;
        }

        public DriverContext Ctx => _dCtx;

        public bool IsDriverRunning => _isDriverRunning == 1;

        public bool IsDriverDisposed => _isDriverRunning == -1;

        /// <summary>
        /// Start Aeron driver with the given <see cref="DriverContext"/> instance. 
        /// </summary>
        public static AeronDriver Start(DriverContext driverContext)
        {
            if (driverContext == null)
                throw new ArgumentNullException(nameof(driverContext));

            driverContext.ConcludeAeronDirectory();

            // Start native embedded driver. Due to C# ctor execution order the embedded
            // driver must be already running before we call the ctor.
            var native = EnsureRunningDriver(driverContext);

            return new AeronDriver(driverContext, native);
        }

        public static bool IsDriverActive(string directory, long timeoutMs = 10000)
        {
            var result = AeronIsDriverActive(directory, timeoutMs,
                Marshal.GetFunctionPointerForDelegate(DriverContext.NOOP_LOGGER_NATIVE));
            GC.KeepAlive(DriverContext.NOOP_LOGGER_NATIVE);
            return result;
        }

        /// <summary>
        /// Check if there is an active driver at the <see cref="DriverContext.AeronDirectoryName()"/>>
        /// or start an embedded driver at the same location.
        /// </summary>
        private static NativeDriver EnsureRunningDriver(DriverContext dCtx)
        {
            var aeronDir = dCtx.AeronDirectoryName();
            var driverTimeoutMs = dCtx.DriverTimeoutMs();
            if (!dCtx.UseActiveDriverIfPresent() && IsDriverActive(aeronDir, driverTimeoutMs))
            {
                for (int i = 0; i < 12; i++) // 20% more max
                {
                    Thread.Sleep((int) Math.Min(driverTimeoutMs / 10, int.MaxValue));
                    if (!IsDriverActive(aeronDir, driverTimeoutMs))
                    {
                        break;
                    }

                    if (i == 11)
                    {
                        var errorMsg =
                            $"Cannot start media driver: there is an active driver in the directory {aeronDir}";
                        dCtx.LogError(errorMsg);
                        throw new InvalidOperationException(errorMsg);
                    }
                }
            }

            if (!IsDriverActive(aeronDir, driverTimeoutMs))
            {
                var native = StartEmbedded(dCtx);
                if (!IsDriverActive(aeronDir, driverTimeoutMs))
                    throw new MediaDriverException("Cannot start media driver");
                return native;
            }

            return default;
        }

        private static T AssertEqual<T>(string name, T config, T fromDriver)
        {
            if (!EqualityComparer<T>.Default.Equals(config, fromDriver))
                throw new MediaDriverException(
                    $"Config {name}: value {config} is not set, the value from the driver is {fromDriver}");
            return fromDriver;
        }

        private static NativeDriver StartEmbedded(DriverContext dCtx)
        {
            if (!Environment.Is64BitProcess)
                throw new InvalidOperationException("Embedded Aeron Media Driver is not supported on 32 bits");

            var aeronDir = dCtx.AeronDirectoryName();

            if (string.IsNullOrEmpty(aeronDir))
                throw new ArgumentException("Aeron directory must be a valid path.");

            dCtx.LogInfo($"Aeron Media Driver: {AeronVersionFull()}");

            if (AeronDriverContextInit(out var nativeCtx) < 0)
                throw new MediaDriverException($"AeronDriverContextInit: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetDir(nativeCtx, aeronDir) < 0)
                throw new MediaDriverException($"AeronDriverContextSetDir: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo(
                $"Starting embedded C media driver at dir: {AssertEqual("AeronDirectoryName", aeronDir, AeronDriverContextGetDir(nativeCtx))}");

            if (AeronDriverContextSetPrintConfiguration(nativeCtx, dCtx.PrintConfigurationOnStart()) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetPrintConfiguration: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"PrintConfigurationOnStart: " +
                         $"{AssertEqual("PrintConfigurationOnStart", dCtx.PrintConfigurationOnStart(), AeronDriverContextGetPrintConfiguration(nativeCtx))}");

            if (AeronDriverContextSetDirDeleteOnStart(nativeCtx, dCtx.DirDeleteOnStart()) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDirDeleteOnStart: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"DirDeleteOnStart: " +
                         $"{AssertEqual("DirDeleteOnStart", dCtx.DirDeleteOnStart(), AeronDriverContextGetDirDeleteOnStart(nativeCtx))}");

            if (AeronDriverContextSetDirDeleteOnShutdown(nativeCtx, dCtx.DirDeleteOnShutdown()) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDirDeleteOnShutdown: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"DirDeleteOnShutdown: " +
                         $"{AssertEqual("DirDeleteOnShutdown", dCtx.DirDeleteOnShutdown(), AeronDriverContextGetDirDeleteOnShutdown(nativeCtx))}");

            if (AeronDriverContextSetTermBufferLength(nativeCtx, (IntPtr) dCtx.TermBufferLength()) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetTermBufferLength: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"TermBufferLength: " +
                         $"{AssertEqual("TermBufferLength", (IntPtr) dCtx.TermBufferLength(), AeronDriverContextGetTermBufferLength(nativeCtx))}");

            // if (AeronDriverContextSetPublicationTermWindowLength(nativeCtx,
            //     (IntPtr) dCtx.PublicationTermWindowLength()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetPublicationTermWindowLength: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"PublicationTermWindowLength: " +
            //              $"{AssertEqual("PublicationTermWindowLength", (IntPtr) dCtx.PublicationTermWindowLength(), AeronDriverContextGetPublicationTermWindowLength(nativeCtx))}");
            //
            // if (AeronDriverContextSetRcvInitialWindowLength(nativeCtx, (IntPtr) dCtx.InitialWindowLength()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetRcvInitialWindowLength: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"InitialWindowLength: " +
            //              $"{AssertEqual("InitialWindowLength", (IntPtr) dCtx.InitialWindowLength(), AeronDriverContextGetRcvInitialWindowLength(nativeCtx))}");
            //
            // if (AeronDriverContextSetSocketSoRcvbuf(nativeCtx, (IntPtr) dCtx.SocketRcvbufLength()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetSocketSoRcvbuf: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"SocketRcvbufLength: " +
            //              $"{AssertEqual("SocketRcvbufLength", (IntPtr) dCtx.SocketRcvbufLength(), AeronDriverContextGetSocketSoRcvbuf(nativeCtx))}");
            //
            // if (AeronDriverContextSetSocketSoSndbuf(nativeCtx, (IntPtr) dCtx.SocketSndbufLength()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetSocketSoSndbuf: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"SocketSndbufLength: " +
            //              $"{AssertEqual("SocketSndbufLength", (IntPtr) dCtx.SocketSndbufLength(), AeronDriverContextGetSocketSoSndbuf(nativeCtx))}");
            //
            // if (AeronDriverContextSetMtuLength(nativeCtx, (IntPtr) dCtx.MtuLength()) < 0)
            //     throw new MediaDriverException($"AeronDriverContextSetMtuLength: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"MtuLength: " +
            //              $"{AssertEqual("MtuLength", (IntPtr) dCtx.MtuLength(), AeronDriverContextGetMtuLength(nativeCtx))}");

            // if (AeronDriverContextSetDriverTimeoutMs(nativeCtx, (ulong) dCtx.DriverTimeoutMs()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetDriverTimeoutMs: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"DriverTimeoutMs: " +
            //              $"{AssertEqual("DriverTimeoutMs", (ulong) dCtx.DriverTimeoutMs(), AeronDriverContextGetDriverTimeoutMs(nativeCtx))}");
            //
            // if (AeronDriverContextSetClientLivenessTimeoutNs(nativeCtx, (ulong) dCtx.ClientLivenessTimeoutNs()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetClientLivenessTimeoutNs: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"ClientLivenessTimeoutNs: " +
            //              $"{AssertEqual("ClientLivenessTimeoutNs", (ulong) dCtx.ClientLivenessTimeoutNs(), AeronDriverContextGetClientLivenessTimeoutNs(nativeCtx))}");

            // if (AeronDriverContextSetPublicationUnblockTimeoutNs(nativeCtx,
            //     (ulong) dCtx.PublicationUnblockTimeoutNs()) < 0)
            //     throw new MediaDriverException(
            //         $"AeronDriverContextSetPublicationUnblockTimeoutNs: ({AeronErrcode()}) {AeronErrmsg()}");
            // dCtx.LogInfo($"PublicationUnblockTimeoutNs: " +
            //              $"{AssertEqual("PublicationUnblockTimeoutNs", (ulong) dCtx.PublicationUnblockTimeoutNs(), AeronDriverContextGetPublicationUnblockTimeoutNs(nativeCtx))}");

            if (AeronDriverContextSetThreadingMode(nativeCtx, dCtx.ThreadingMode()) < 0)
                throw new MediaDriverException($"AeronDriverContextSetDir: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"ThreadingMode: " +
                         $"{AssertEqual("ThreadingMode", dCtx.ThreadingMode(), AeronDriverContextGetThreadingMode(nativeCtx))}");

            if (AeronDriverContextSetConductorIdleStrategy(nativeCtx, dCtx.ConductorIdleStrategy().Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetConductorIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"ConductorIdleStrategy: " +
                         $"{AssertEqual("ConductorIdleStrategy", dCtx.ConductorIdleStrategy().Name, AeronDriverContextGetConductorIdleStrategy(nativeCtx))}");

            if (AeronDriverContextSetSenderIdleStrategy(nativeCtx, dCtx.SenderIdleStrategy().Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSenderIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"SenderIdleStrategy: " +
                         $"{AssertEqual("SenderIdleStrategy", dCtx.SenderIdleStrategy().Name, AeronDriverContextGetSenderIdleStrategy(nativeCtx))}");

            if (AeronDriverContextSetReceiverIdleStrategy(nativeCtx, dCtx.ReceiverIdleStrategy().Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetReceiverIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"ReceiverIdleStrategy: " +
                         $"{AssertEqual("ReceiverIdleStrategy", dCtx.ReceiverIdleStrategy().Name, AeronDriverContextGetReceiverIdleStrategy(nativeCtx))}");

            if (AeronDriverContextSetSharednetworkIdleStrategy(nativeCtx, dCtx.SharedNetworkIdleStrategy().Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetConductorIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"SharedNetworkIdleStrategy: " +
                         $"{AssertEqual("SharedNetworkIdleStrategy", dCtx.SharedNetworkIdleStrategy().Name, AeronDriverContextGetSharednetworkIdleStrategy(nativeCtx))}");

            if (AeronDriverContextSetSharedIdleStrategy(nativeCtx, dCtx.SharedIdleStrategy().Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSharedIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");
            dCtx.LogInfo($"SharedIdleStrategy: " +
                         $"{AssertEqual("SharedIdleStrategy", dCtx.SharedIdleStrategy().Name, AeronDriverContextGetSharedIdleStrategy(nativeCtx))}");

            if (AeronDriverInit(out var nativeDriver, nativeCtx) < 0)
                throw new MediaDriverException($"AeronDriverInit: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverStart(nativeDriver, false) < 0)
                throw new MediaDriverException($"AeronDriverStart: ({AeronErrcode()}) {AeronErrmsg()}");
            
            // TODO get aeron directory from context?

            return new NativeDriver(nativeDriver, nativeCtx);
        }

        private void Dispose(bool disposing)
        {
            var wasRunning = Interlocked.CompareExchange(ref _isDriverRunning, 1, -1);
            if (wasRunning == 0)
                return;
            if (wasRunning == -1)
                throw new ObjectDisposedException(nameof(AeronDriver), "Media driver is already disposed");

            AppDomain.CurrentDomain.ProcessExit -= _processExitHandler;

            _dCtx.LogInfo($"Disposing media driver at: {_dCtx.AeronDirectoryName()}");

            if (AeronDriverClose(_native.Driver) < 0)
            {
                if (!disposing)
                    return;

                throw new MediaDriverException($"AeronDriverClose: ({AeronErrcode()}) {AeronErrmsg()}");
            }

            if (AeronDriverContextClose(_native.Ctx) < 0)
            {
                if (!disposing)
                    return;

                throw new MediaDriverException($"AeronDriverContextClose: ({AeronErrcode()}) {AeronErrmsg()}");
            }
        }

        ~AeronDriver()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void OnCurrentDomainOnProcessExit(object? sender, EventArgs args)
        {
            Dispose();
        }
    }
}
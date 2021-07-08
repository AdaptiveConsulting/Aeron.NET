using System;
using System.Collections.Generic;
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
            
            // Start native embedded driver. Due to C# ctor execution order the embedded
            // driver must be already running before we call the ctor.
            var native = EnsureRunningDriver(driverContext);

            return new AeronDriver(driverContext, native);
        }

        public static bool IsDriverActive(string directory, long timeoutMs = 10000)
        {
            return AeronIsDriverActive(directory, timeoutMs, DriverContext.NOOP_LOGGER_NATIVE_PTR);
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
                    Thread.Sleep((int)Math.Min(driverTimeoutMs / 10, int.MaxValue));
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

        private static void ThrowOnNativeError(string name)
        {
            throw new MediaDriverException($"{name}: ({AeronErrcode()}) {AeronErrmsg()}");
        }

        private static NativeDriver StartEmbedded(DriverContext dCtx)
        {
            if (!Environment.Is64BitProcess)
                throw new InvalidOperationException("Embedded Aeron Media Driver is not supported on 32 bits");

            var aeronDir = dCtx.AeronDirectoryName();

            void LogAssertParameter<T>(string parameterName, T config, T fromDriver)
            {
                const string prefix = "AeronDriverContextSet";
                if (parameterName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    parameterName = parameterName.Substring(prefix.Length);

                if (!EqualityComparer<T>.Default.Equals(config, fromDriver))
                    throw new MediaDriverException($"Config {parameterName}: value {config} is not set, the value from the driver is {fromDriver}");

                if (typeof(T) == typeof(long) || typeof(T) == typeof(int))
                    dCtx.LogInfo($"{parameterName}: {((long)(object)fromDriver!):N0}");
                else if (typeof(T) == typeof(ulong) || typeof(T) == typeof(uint))
                    dCtx.LogInfo($"{parameterName}: {((ulong)(object)fromDriver!):N0}");
                else
                    dCtx.LogInfo($"{parameterName}: {fromDriver}");
            }

            if (string.IsNullOrEmpty(aeronDir))
                throw new ArgumentException("Aeron directory must be a valid path.");

            dCtx.LogInfo($"Aeron Media Driver: {AeronVersionFull()}");

            if (AeronDriverContextInit(out var nativeCtx) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextInit));

            if (AeronDriverContextSetDir(nativeCtx, aeronDir) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetDir));
            LogAssertParameter(nameof(AeronDriverContextSetDir), aeronDir, AeronDriverContextGetDir(nativeCtx));

            if (AeronDriverContextSetPrintConfiguration(nativeCtx, dCtx.PrintConfigurationOnStart()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetPrintConfiguration));
            LogAssertParameter(nameof(AeronDriverContextSetPrintConfiguration), dCtx.PrintConfigurationOnStart(), AeronDriverContextGetPrintConfiguration(nativeCtx));

            if (AeronDriverContextSetDirDeleteOnStart(nativeCtx, dCtx.DirDeleteOnStart()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetDirDeleteOnStart));
            LogAssertParameter(nameof(AeronDriverContextSetDirDeleteOnStart), dCtx.DirDeleteOnStart(), AeronDriverContextGetDirDeleteOnStart(nativeCtx));

            if (AeronDriverContextSetDirDeleteOnShutdown(nativeCtx, dCtx.DirDeleteOnShutdown()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetDirDeleteOnShutdown));
            LogAssertParameter(nameof(AeronDriverContextSetDirDeleteOnShutdown), dCtx.DirDeleteOnShutdown(), AeronDriverContextGetDirDeleteOnShutdown(nativeCtx));

            if (AeronDriverContextSetTermBufferLength(nativeCtx, (IntPtr)dCtx.TermBufferLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetTermBufferLength));
            LogAssertParameter(nameof(AeronDriverContextSetTermBufferLength), dCtx.TermBufferLength(), (long)AeronDriverContextGetTermBufferLength(nativeCtx));

            if (AeronDriverContextSetPublicationTermWindowLength(nativeCtx, (IntPtr)dCtx.PublicationTermWindowLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetPublicationTermWindowLength));
            LogAssertParameter(nameof(AeronDriverContextSetPublicationTermWindowLength), dCtx.PublicationTermWindowLength(), (long)AeronDriverContextGetPublicationTermWindowLength(nativeCtx));

            if (AeronDriverContextSetRcvInitialWindowLength(nativeCtx, (IntPtr)dCtx.InitialWindowLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetRcvInitialWindowLength));
            LogAssertParameter(nameof(AeronDriverContextSetRcvInitialWindowLength), dCtx.InitialWindowLength(), (long)AeronDriverContextGetRcvInitialWindowLength(nativeCtx));

            if (AeronDriverContextSetSocketSoRcvbuf(nativeCtx, (IntPtr)dCtx.SocketRcvbufLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetSocketSoRcvbuf));
            LogAssertParameter(nameof(AeronDriverContextSetSocketSoRcvbuf), dCtx.SocketRcvbufLength(), (long)AeronDriverContextGetSocketSoRcvbuf(nativeCtx));

            if (AeronDriverContextSetSocketSoSndbuf(nativeCtx, (IntPtr)dCtx.SocketSndbufLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetSocketSoSndbuf));
            LogAssertParameter(nameof(AeronDriverContextSetSocketSoSndbuf), dCtx.SocketSndbufLength(), (long)AeronDriverContextGetSocketSoSndbuf(nativeCtx));

            if (AeronDriverContextSetMtuLength(nativeCtx, (IntPtr)dCtx.MtuLength()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetMtuLength));
            LogAssertParameter(nameof(AeronDriverContextSetMtuLength), dCtx.MtuLength(), (long)AeronDriverContextGetMtuLength(nativeCtx));

            if (AeronDriverContextSetDriverTimeoutMs(nativeCtx, (ulong)dCtx.DriverTimeoutMs()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetDriverTimeoutMs));
            LogAssertParameter(nameof(AeronDriverContextSetDriverTimeoutMs), (ulong)dCtx.DriverTimeoutMs(), AeronDriverContextGetDriverTimeoutMs(nativeCtx));

            if (AeronDriverContextSetClientLivenessTimeoutNs(nativeCtx, (ulong)dCtx.ClientLivenessTimeoutNs()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetClientLivenessTimeoutNs));
            LogAssertParameter(nameof(AeronDriverContextSetClientLivenessTimeoutNs), (ulong)dCtx.ClientLivenessTimeoutNs(), AeronDriverContextGetClientLivenessTimeoutNs(nativeCtx));

            if (AeronDriverContextSetPublicationUnblockTimeoutNs(nativeCtx, (ulong)dCtx.PublicationUnblockTimeoutNs()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetPublicationUnblockTimeoutNs));
            LogAssertParameter(nameof(AeronDriverContextSetPublicationUnblockTimeoutNs), (ulong)dCtx.PublicationUnblockTimeoutNs(), AeronDriverContextGetPublicationUnblockTimeoutNs(nativeCtx));

            if (AeronDriverContextSetThreadingMode(nativeCtx, dCtx.ThreadingMode()) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetThreadingMode));
            LogAssertParameter(nameof(AeronDriverContextSetThreadingMode), dCtx.ThreadingMode(), AeronDriverContextGetThreadingMode(nativeCtx));

            if (AeronDriverContextSetConductorIdleStrategy(nativeCtx, dCtx.ConductorIdleStrategy().Name) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetConductorIdleStrategy));
            LogAssertParameter(nameof(AeronDriverContextSetConductorIdleStrategy), dCtx.ConductorIdleStrategy().Name, AeronDriverContextGetConductorIdleStrategy(nativeCtx));

            if (AeronDriverContextSetSenderIdleStrategy(nativeCtx, dCtx.SenderIdleStrategy().Name) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetSenderIdleStrategy));
            LogAssertParameter(nameof(AeronDriverContextSetSenderIdleStrategy), dCtx.SenderIdleStrategy().Name, AeronDriverContextGetSenderIdleStrategy(nativeCtx));

            if (AeronDriverContextSetReceiverIdleStrategy(nativeCtx, dCtx.ReceiverIdleStrategy().Name) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetReceiverIdleStrategy));
            LogAssertParameter(nameof(AeronDriverContextSetReceiverIdleStrategy), dCtx.ReceiverIdleStrategy().Name, AeronDriverContextGetReceiverIdleStrategy(nativeCtx));

            if (AeronDriverContextSetSharednetworkIdleStrategy(nativeCtx, dCtx.SharedNetworkIdleStrategy().Name) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetSharednetworkIdleStrategy));
            LogAssertParameter(nameof(AeronDriverContextSetSharednetworkIdleStrategy), dCtx.SharedNetworkIdleStrategy().Name, AeronDriverContextGetSharednetworkIdleStrategy(nativeCtx));

            if (AeronDriverContextSetSharedIdleStrategy(nativeCtx, dCtx.SharedIdleStrategy().Name) < 0)
                ThrowOnNativeError(nameof(AeronDriverContextSetSharedIdleStrategy));
            LogAssertParameter(nameof(AeronDriverContextSetSharedIdleStrategy), dCtx.SharedIdleStrategy().Name, AeronDriverContextGetSharedIdleStrategy(nativeCtx));

            if (AeronDriverInit(out var nativeDriver, nativeCtx) < 0)
                ThrowOnNativeError(nameof(AeronDriverInit));

            if (AeronDriverStart(nativeDriver, false) < 0)
                ThrowOnNativeError(nameof(AeronDriverStart));

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

                ThrowOnNativeError(nameof(AeronDriverClose));
            }

            if (AeronDriverContextClose(_native.Ctx) < 0)
            {
                if (!disposing)
                    return;

                ThrowOnNativeError(nameof(AeronDriverContextClose));
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

using System;
using System.Runtime.InteropServices;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Driver.Native
{
    public partial class AeronDriver
    {
        public static class DriverConfiguration
        {
            /// <summary>
            /// Should the driver print it's configuration on start to System#out at the end of conclude().
            /// </summary>
            public const string PRINT_CONFIGURATION_ON_START_PROP_NAME = "aeron.print.configuration";

            public const bool PRINT_CONFIGURATION_ON_START_DEFAULT = false;

            /// <summary>
            /// Should the driver print it's configuration on start to System#out at the end of conclude().
            /// </summary>
            public static bool PrintConfigurationOnStart()
            {
                string value = Config.GetProperty(PRINT_CONFIGURATION_ON_START_PROP_NAME);
                if (null != value) return bool.Parse(value);
                return PRINT_CONFIGURATION_ON_START_DEFAULT;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on startup even if the timestamp is current. This is useful for testing of forced restart.
            /// </summary>
            public const string DIR_DELETE_ON_START_PROP_NAME = "aeron.dir.delete.on.start";

            public const bool DIR_DELETE_ON_START_DEFAULT = false;

            /// <summary>
            /// Should the media driver delete the Aeron directory on startup even if the timestamp is current. This is useful for testing of forced restart.
            /// </summary>
            public static bool DirDeleteOnStart()
            {
                string value = Config.GetProperty(DIR_DELETE_ON_START_PROP_NAME);
                if (null != value) return bool.Parse(value);
                return DIR_DELETE_ON_START_DEFAULT;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on shutdown.
            /// </summary>
            public const string DIR_DELETE_ON_SHUTDOWN_PROP_NAME = "aeron.dir.delete.on.shutdown";

            public const bool DIR_DELETE_ON_SHUTDOWN_DEFAULT = false;

            /// <summary>
            /// Should the media driver delete the Aeron directory on shutdown.
            /// </summary>
            public static bool DirDeleteOnShutdown()
            {
                string value = Config.GetProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME);
                if (null != value)
                {
                    return bool.Parse(value);
                }

                return DIR_DELETE_ON_SHUTDOWN_DEFAULT;
            }

            /// <summary>
            /// Timeout in nanoseconds after which a driver will consider a client dead if it has not received an keepalive. 
            /// </summary>
            public const string CLIENT_LIVENESS_TIMEOUT_PROP_NAME = "aeron.client.liveness.timeout";

            public const long CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS = 10 * 1_000_000_000L;

            /// <summary>
            /// Timeout in nanoseconds after which a driver will consider a client dead if it has not received an keepalive. 
            /// </summary>
            public static long ClientLivenessTimeoutNs()
            {
                return Config.GetLong(CLIENT_LIVENESS_TIMEOUT_PROP_NAME, CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS);
            }

            /// <summary>
            /// Timeout in nanoseconds after which a publication will be unblocked if an offer is partially complete to allow other publishers to make progress.  
            /// </summary>
            public const string PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME = "aeron.client.liveness.timeout";

            public const long PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS = 15 * 1_000_000_000L;

            /// <summary>
            /// Timeout in nanoseconds after which a publication will be unblocked if an offer is partially complete to allow other publishers to make progress.  
            /// </summary>
            public static long PublicationUnblockTimeoutNs()
            {
                return Config.GetLong(PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME, PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS);
            }

            /// <summary>
            /// Length (in bytes) of the log buffers for UDP publication terms.
            /// </summary>
            public const string TERM_BUFFER_LENGTH_PROP_NAME = "aeron.term.buffer.length";

            public const int TERM_BUFFER_LENGTH_DEFAULT = 16 * 1024 * 1024;

            /// <summary>
            /// Length (in bytes) of the log buffers for UDP publication terms.
            /// </summary>
            public static int TermBufferLength()
            {
                return Config.GetInteger(TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_DEFAULT);
            }

            /// <summary>
            /// Default length in bytes for a term buffer window on a network publication. 
            /// </summary>
            public const string PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.publication.term.window.length";

            public const int PUBLICATION_TERM_WINDOW_LENGTH = 0;

            /// <summary>
            /// Default length in bytes for a term buffer window on a network publication. 
            /// </summary>
            public static int PublicationTermWindowLength()
            {
                return Config.GetInteger(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, PUBLICATION_TERM_WINDOW_LENGTH);
            }

            /// <summary>
            /// Window for flow control of in-flight bytes between sender and receiver on a stream. This needs to be sufficient for BDP (Bandwidth Delay Product) and be greater than MTU. 
            /// </summary>
            public const string INITIAL_WINDOW_LENGTH_PROP_NAME = "aeron.rcv.initial.window.length";

            public const int INITIAL_WINDOW_LENGTH_DEFAULT = 128 * 1024;

            /// <summary>
            /// Window for flow control of in-flight bytes between sender and receiver on a stream. This needs to be sufficient for BDP (Bandwidth Delay Product) and be greater than MTU. 
            /// </summary>
            public static int InitialWindowLength()
            {
                return Config.GetInteger(INITIAL_WINDOW_LENGTH_PROP_NAME, INITIAL_WINDOW_LENGTH_DEFAULT);
            }

            /// <summary>
            /// Length in bytes for the SO_RCVBUF, 0 means use OS default. This needs to be larger than Receiver Window. 
            /// </summary>
            public const string SOCKET_RCVBUF_LENGTH_PROP_NAME = "aeron.socket.so_rcvbuf";

            public const int SOCKET_RCVBUF_LENGTH_DEFAULT = 128 * 1024;

            /// <summary>
            /// Length in bytes for the SO_RCVBUF, 0 means use OS default. This needs to be larger than Receiver Window.
            /// </summary>
            public static int SocketRcvbufLength()
            {
                return Config.GetInteger(SOCKET_RCVBUF_LENGTH_PROP_NAME, SOCKET_RCVBUF_LENGTH_DEFAULT);
            }

            /// <summary>
            /// Length in bytes for the SO_SNDBUF, 0 means use OS default. This needs to be larger than Receiver Window. 
            /// </summary>
            public const string SOCKET_SNDBUF_LENGTH_PROP_NAME = "aeron.socket.so_rcvbuf";

            public const int SOCKET_SNDBUF_LENGTH_DEFAULT = 0;

            /// <summary>
            /// Length in bytes for the SO_SNDBUF, 0 means use OS default. This needs to be larger than Receiver Window. 
            /// </summary>
            public static int SocketSndbufLength()
            {
                return Config.GetInteger(SOCKET_SNDBUF_LENGTH_PROP_NAME, SOCKET_SNDBUF_LENGTH_DEFAULT);
            }

            /// <summary>
            /// Maximum length of a message fragment including Aeron data frame header for transmission in a network packet.
            /// This can be a larger than an Ethernet MTU provided it is smaller than the maximum UDP payload length.
            /// Larger lengths enable batching and reducing syscalls at the expense of more likely loss.  
            /// </summary>
            public const string MTU_LENGTH_PROP_NAME = "aeron.mtu.length";

            public const int MTU_LENGTH_DEFAULT = 1408;

            /// <summary>
            /// Maximum length of a message fragment including Aeron data frame header for transmission in a network packet.
            /// This can be a larger than an Ethernet MTU provided it is smaller than the maximum UDP payload length.
            /// Larger lengths enable batching and reducing syscalls at the expense of more likely loss.  
            /// </summary>
            public static int MtuLength()
            {
                return Config.GetInteger(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);
            }

            /// <summary>
            /// DEDICATED is a thread for each of the Conductor, Sender, and Receiver Agents. SHARED is one thread for all three Agents.
            /// SHARED_NETWORK is one thread for the conductor and one shared for the Sender and Receiver Agents. INVOKER is a mode with
            /// no threads, i.e. the client is responsible for using the MediaDriver.Context.driverAgentInvoker() to invoke the duty cycle directly.
            /// </summary>
            public const string THREADING_MODE_PROP_NAME = "aeron.threading.mode";

            public const AeronThreadingModeEnum THREADING_MODE_DEFAULT =
                AeronThreadingModeEnum.AeronThreadingModeDedicated;

            /// <summary>
            /// DEDICATED is a thread for each of the Conductor, Sender, and Receiver Agents. SHARED is one thread for all three Agents.
            /// SHARED_NETWORK is one thread for the conductor and one shared for the Sender and Receiver Agents. INVOKER is a mode with
            /// no threads, i.e. the client is responsible for using the MediaDriver.Context.driverAgentInvoker() to invoke the duty cycle directly.
            /// </summary>
            public static AeronThreadingModeEnum ThreadingMode()
            {
                string value = Config.GetProperty(THREADING_MODE_PROP_NAME, "dedicated").ToUpper();
                switch (value)
                {
                    case "DEDICATED":
                        return AeronThreadingModeEnum.AeronThreadingModeDedicated;
                    case "SHARED":
                        return AeronThreadingModeEnum.AeronThreadingModeShared;
                    case "SHARED_NETWORK":
                        return AeronThreadingModeEnum.AeronThreadingModeSharedNetwork;
                    case "INVOKER":
                        throw new NotSupportedException();
                }

                return THREADING_MODE_DEFAULT;
            }

            public const string CONDUCTOR_IDLE_STRATEGY_PROP_NAME = "aeron.conductor.idle.strategy";
            public static DriverIdleStrategy CONDUCTOR_IDLE_STRATEGY_DEFAULT = DriverIdleStrategy.BACKOFF;

            public static DriverIdleStrategy ConductorIdleStrategy()
            {
                string value = Config.GetProperty(CONDUCTOR_IDLE_STRATEGY_PROP_NAME,
                    CONDUCTOR_IDLE_STRATEGY_DEFAULT.Name);
                return DriverIdleStrategy.FromString(value);
            }

            public const string SENDER_IDLE_STRATEGY_PROP_NAME = "aeron.sender.idle.strategy";
            public static DriverIdleStrategy SENDER_IDLE_STRATEGY_DEFAULT = DriverIdleStrategy.BACKOFF;

            public static DriverIdleStrategy SenderIdleStrategy()
            {
                string value = Config.GetProperty(SENDER_IDLE_STRATEGY_PROP_NAME, SENDER_IDLE_STRATEGY_DEFAULT.Name);
                return DriverIdleStrategy.FromString(value);
            }

            public const string RECEIVER_IDLE_STRATEGY_PROP_NAME = "aeron.receiver.idle.strategy";
            public static DriverIdleStrategy RECEIVER_IDLE_STRATEGY_DEFAULT = DriverIdleStrategy.BACKOFF;

            public static DriverIdleStrategy ReceiverIdleStrategy()
            {
                string value =
                    Config.GetProperty(RECEIVER_IDLE_STRATEGY_PROP_NAME, RECEIVER_IDLE_STRATEGY_DEFAULT.Name);
                return DriverIdleStrategy.FromString(value);
            }

            public const string SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME = "aeron.sharednetwork.idle.strategy";
            public static DriverIdleStrategy SHARED_NETWORK_IDLE_STRATEGY_DEFAULT = DriverIdleStrategy.BACKOFF;

            public static DriverIdleStrategy SharedNetworkIdleStrategy()
            {
                string value = Config.GetProperty(SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME,
                    SHARED_NETWORK_IDLE_STRATEGY_DEFAULT.Name);
                return DriverIdleStrategy.FromString(value);
            }

            public const string SHARED_IDLE_STRATEGY_PROP_NAME = "aeron.shared.idle.strategy";
            public static readonly DriverIdleStrategy SHARED_IDLE_STRATEGY_DEFAULT = DriverIdleStrategy.BACKOFF;

            public static DriverIdleStrategy SharedIdleStrategy()
            {
                string value = Config.GetProperty(SHARED_IDLE_STRATEGY_PROP_NAME, SHARED_IDLE_STRATEGY_DEFAULT.Name);
                return DriverIdleStrategy.FromString(value);
            }
        }

        /// <summary>
        /// Configures <see cref="AeronDriver"/> parameters.
        /// </summary>
        public class DriverContext
        {
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void LogFuncDelegate([MarshalAs(UnmanagedType.LPStr)] string pChar);

            private static readonly Action<string> NOOP_LOGGER = _ => { };
            internal static readonly LogFuncDelegate NOOP_LOGGER_NATIVE = new LogFuncDelegate(NOOP_LOGGER);
            internal static readonly IntPtr NOOP_LOGGER_NATIVE_PTR = Marshal.GetFunctionPointerForDelegate(DriverContext.NOOP_LOGGER_NATIVE);

            private Action<string> _loggerInfo = NOOP_LOGGER;
            private Action<string>? _loggerWarn;
            private Action<string>? _loggerError;
            private bool _useActiveDriverIfPresent;

            private long _driverTimeoutMs = Aeron.Context.DRIVER_TIMEOUT_MS;
            private bool _printConfigurationOnStart;
            private bool _dirDeleteOnShutdown = DriverConfiguration.DirDeleteOnShutdown();
            private bool _dirDeleteOnStart = DriverConfiguration.DirDeleteOnStart();
            private long _clientLivenessTimeoutNs = DriverConfiguration.ClientLivenessTimeoutNs();
            private long _publicationUnblockTimeoutNs = DriverConfiguration.PublicationUnblockTimeoutNs();
            private int _termBufferLength = DriverConfiguration.TermBufferLength();
            private int _publicationTermWindowLength = DriverConfiguration.PublicationTermWindowLength();
            private int _initialWindowLength = DriverConfiguration.InitialWindowLength();
            private int _socketRcvbufLength = DriverConfiguration.SocketRcvbufLength();
            private int _socketSndbufLength = DriverConfiguration.SocketSndbufLength();
            private int _mtuLength = DriverConfiguration.MtuLength();
            private AeronThreadingModeEnum _threadingMode = DriverConfiguration.ThreadingMode();
            private DriverIdleStrategy _conductorIdleStrategy = DriverConfiguration.ConductorIdleStrategy();
            private DriverIdleStrategy _senderIdleStrategy = DriverConfiguration.SenderIdleStrategy();
            private DriverIdleStrategy _receiverIdleStrategy = DriverConfiguration.ReceiverIdleStrategy();
            private DriverIdleStrategy _sharedNetworkIdleStrategy = DriverConfiguration.SharedNetworkIdleStrategy();
            private DriverIdleStrategy _sharedIdleStrategy = DriverConfiguration.SharedIdleStrategy();
            private string? _aeronDirectoryName;

            public DriverContext LoggerInfo(Action<string> logger)
            {
                _loggerInfo = logger;
                return this;
            }

            public Action<string> LoggerInfo() => _loggerInfo;
            internal void LogInfo(string message) => LoggerInfo().Invoke(message);

            public DriverContext LoggerWarning(Action<string> logger)
            {
                _loggerWarn = logger;
                return this;
            }

            public Action<string> LoggerWarning() => _loggerWarn ?? LoggerInfo();
            internal void LogWarning(string message) => LoggerWarning().Invoke(message);

            public DriverContext LoggerError(Action<string> logger)
            {
                _loggerError = logger;
                return this;
            }

            public Action<string> LoggerError() => _loggerError ?? LoggerWarning();
            internal void LogError(string message) => LoggerError().Invoke(message);

            public DriverContext UseActiveDriverIfPresent(bool useActiveDriverIfPresent)
            {
                _useActiveDriverIfPresent = useActiveDriverIfPresent;
                return this;
            }

            public bool UseActiveDriverIfPresent()
            {
                return _useActiveDriverIfPresent;
            }

            /// <summary>
            /// Should the driver print it's configuration on start to System#out at the end of conclude().
            /// </summary>
            /// <seealso cref="DriverConfiguration.PRINT_CONFIGURATION_ON_START_PROP_NAME"/>
            public DriverContext PrintConfigurationOnStart(bool printConfigurationOnStart)
            {
                _printConfigurationOnStart = printConfigurationOnStart;
                return this;
            }

            /// <summary>
            /// Should the driver print it's configuration on start to System#out at the end of conclude().
            /// </summary>
            public bool PrintConfigurationOnStart()
            {
                return _printConfigurationOnStart;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on startup even if the timestamp is current. This is useful for testing of forced restart.
            /// </summary>
            /// <seealso cref="DriverConfiguration.DIR_DELETE_ON_START_PROP_NAME"/>
            public DriverContext DirDeleteOnStart(bool dirDeleteOnStart)
            {
                _dirDeleteOnStart = dirDeleteOnStart;
                return this;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on startup even if the timestamp is current. This is useful for testing of forced restart.
            /// </summary>
            public bool DirDeleteOnStart()
            {
                return _dirDeleteOnStart;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on shutdown.
            /// </summary>
            /// <seealso cref="DriverConfiguration.DIR_DELETE_ON_SHUTDOWN_PROP_NAME"/>
            public DriverContext DirDeleteOnShutdown(bool dirDeleteOnShutdown)
            {
                _dirDeleteOnShutdown = dirDeleteOnShutdown;
                return this;
            }

            /// <summary>
            /// Should the media driver delete the Aeron directory on shutdown.
            /// </summary>
            public bool DirDeleteOnShutdown()
            {
                return _dirDeleteOnShutdown;
            }

            /// <summary>
            /// Timeout in nanoseconds after which a driver will consider a client dead if it has not received an keepalive. 
            /// </summary>
            /// <seealso cref="DriverConfiguration.CLIENT_LIVENESS_TIMEOUT_PROP_NAME"/>
            public DriverContext ClientLivenessTimeoutNs(long clientLivenessTimeoutNs)
            {
                _clientLivenessTimeoutNs = clientLivenessTimeoutNs;
                return this;
            }

            /// <summary>
            /// Timeout in nanoseconds after which a driver will consider a client dead if it has not received an keepalive. 
            /// </summary>
            public long ClientLivenessTimeoutNs()
            {
                return Aeron.Context.CheckDebugTimeout(_clientLivenessTimeoutNs, TimeUnit.NANOSECONDS, nameof(ClientLivenessTimeoutNs));
            }

            /// <summary>
            /// Timeout in nanoseconds after which a publication will be unblocked if an offer is partially complete to allow other publishers to make progress.  
            /// </summary>
            /// <seealso cref="DriverConfiguration.PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME"/>
            public DriverContext PublicationUnblockTimeoutNs(long publicationUnblockTimeoutNs)
            {
                _publicationUnblockTimeoutNs = publicationUnblockTimeoutNs;
                return this;
            }

            /// <summary>
            /// Timeout in nanoseconds after which a publication will be unblocked if an offer is partially complete to allow other publishers to make progress.  
            /// </summary>
            public long PublicationUnblockTimeoutNs()
            {
                return Aeron.Context.CheckDebugTimeout(_publicationUnblockTimeoutNs, TimeUnit.NANOSECONDS, nameof(PublicationUnblockTimeoutNs));
            }

            /// <summary>
            /// Length (in bytes) of the log buffers for UDP publication terms.
            /// </summary>
            /// <seealso cref="DriverConfiguration.TERM_BUFFER_LENGTH_PROP_NAME"/>
            public DriverContext TermBufferLength(int termBufferLength)
            {
                _termBufferLength = termBufferLength;
                return this;
            }

            /// <summary>
            /// Length (in bytes) of the log buffers for UDP publication terms.
            /// </summary>
            public int TermBufferLength()
            {
                return _termBufferLength;
            }

            /// <summary>
            /// Default length in bytes for a term buffer window on a network publication. 
            /// </summary>
            /// <seealso cref="DriverConfiguration.PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME"/>
            public DriverContext PublicationTermWindowLength(int publicationTermWindowLength)
            {
                _publicationTermWindowLength = publicationTermWindowLength;
                return this;
            }

            /// <summary>
            /// Default length in bytes for a term buffer window on a network publication. 
            /// </summary>
            public int PublicationTermWindowLength()
            {
                return _publicationTermWindowLength;
            }

            /// <summary>
            /// Window for flow control of in-flight bytes between sender and receiver on a stream. This needs to be sufficient for BDP (Bandwidth Delay Product) and be greater than MTU. 
            /// </summary>
            /// <seealso cref="DriverConfiguration.INITIAL_WINDOW_LENGTH_PROP_NAME"/>
            public DriverContext InitialWindowLength(int initialWindowLength)
            {
                _initialWindowLength = initialWindowLength;
                return this;
            }

            /// <summary>
            /// Window for flow control of in-flight bytes between sender and receiver on a stream. This needs to be sufficient for BDP (Bandwidth Delay Product) and be greater than MTU. 
            /// </summary>
            public int InitialWindowLength()
            {
                return _initialWindowLength;
            }

            /// <summary>
            /// Length in bytes for the SO_RCVBUF, 0 means use OS default. This needs to be larger than Receiver Window.
            /// </summary>
            /// <seealso cref="DriverConfiguration.SOCKET_RCVBUF_LENGTH_PROP_NAME"/>
            public DriverContext SocketRcvbufLength(int socketRcvbufLength)
            {
                _socketRcvbufLength = socketRcvbufLength;
                return this;
            }

            /// <summary>
            /// Length in bytes for the SO_RCVBUF, 0 means use OS default. This needs to be larger than Receiver Window.
            /// </summary>
            public int SocketRcvbufLength()
            {
                return _socketRcvbufLength;
            }

            /// <summary>
            /// Length in bytes for the SO_SNDBUF, 0 means use OS default. This needs to be larger than Receiver Window. 
            /// </summary>
            /// <seealso cref="DriverConfiguration.SOCKET_SNDBUF_LENGTH_PROP_NAME"/>
            public DriverContext SocketSndbufLength(int socketSndbufLength)
            {
                _socketSndbufLength = socketSndbufLength;
                return this;
            }

            /// <summary>
            /// Length in bytes for the SO_SNDBUF, 0 means use OS default. This needs to be larger than Receiver Window. 
            /// </summary>
            public int SocketSndbufLength()
            {
                return _socketSndbufLength;
            }

            /// <summary>
            /// Maximum length of a message fragment including Aeron data frame header for transmission in a network packet.
            /// This can be a larger than an Ethernet MTU provided it is smaller than the maximum UDP payload length.
            /// Larger lengths enable batching and reducing syscalls at the expense of more likely loss.  
            /// </summary>
            /// <seealso cref="DriverConfiguration.MTU_LENGTH_PROP_NAME"/>
            public DriverContext MtuLength(int mtuLength)
            {
                _mtuLength = mtuLength;
                return this;
            }

            /// <summary>
            /// Maximum length of a message fragment including Aeron data frame header for transmission in a network packet.
            /// This can be a larger than an Ethernet MTU provided it is smaller than the maximum UDP payload length.
            /// Larger lengths enable batching and reducing syscalls at the expense of more likely loss.  
            /// </summary>
            public int MtuLength()
            {
                return _mtuLength;
            }

            /// <summary>
            /// DEDICATED is a thread for each of the Conductor, Sender, and Receiver Agents. SHARED is one thread for all three Agents.
            /// SHARED_NETWORK is one thread for the conductor and one shared for the Sender and Receiver Agents. INVOKER is a mode with
            /// no threads, i.e. the client is responsible for using the MediaDriver.Context.driverAgentInvoker() to invoke the duty cycle directly.
            /// </summary>
            /// <seealso cref="DriverConfiguration.THREADING_MODE_PROP_NAME"/>
            public DriverContext ThreadingMode(AeronThreadingModeEnum threadingMode)
            {
                _threadingMode = threadingMode;
                return this;
            }

            /// <summary>
            /// DEDICATED is a thread for each of the Conductor, Sender, and Receiver Agents. SHARED is one thread for all three Agents.
            /// SHARED_NETWORK is one thread for the conductor and one shared for the Sender and Receiver Agents. INVOKER is a mode with
            /// no threads, i.e. the client is responsible for using the MediaDriver.Context.driverAgentInvoker() to invoke the duty cycle directly.
            /// </summary>
            public AeronThreadingModeEnum ThreadingMode()
            {
                return _threadingMode;
            }

            public DriverContext ConductorIdleStrategy(DriverIdleStrategy conductorIdleStrategy)
            {
                _conductorIdleStrategy = conductorIdleStrategy;
                return this;
            }

            public DriverIdleStrategy ConductorIdleStrategy()
            {
                return _conductorIdleStrategy;
            }

            public DriverContext SenderIdleStrategy(DriverIdleStrategy senderIdleStrategy)
            {
                _senderIdleStrategy = senderIdleStrategy;
                return this;
            }

            public DriverIdleStrategy SenderIdleStrategy()
            {
                return _senderIdleStrategy;
            }

            public DriverContext ReceiverIdleStrategy(DriverIdleStrategy receiverIdleStrategy)
            {
                _receiverIdleStrategy = receiverIdleStrategy;
                return this;
            }

            public DriverIdleStrategy ReceiverIdleStrategy()
            {
                return _receiverIdleStrategy;
            }

            public DriverContext SharedNetworkIdleStrategy(DriverIdleStrategy sharedNetworkIdleStrategy)
            {
                _sharedNetworkIdleStrategy = sharedNetworkIdleStrategy;
                return this;
            }

            public DriverIdleStrategy SharedNetworkIdleStrategy()
            {
                return _sharedNetworkIdleStrategy;
            }

            public DriverContext SharedIdleStrategy(DriverIdleStrategy sharedIdleStrategy)
            {
                _sharedIdleStrategy = sharedIdleStrategy;
                return this;
            }

            public DriverIdleStrategy SharedIdleStrategy()
            {
                return _sharedIdleStrategy;
            }

            /// <summary>
            /// The timeout in milliseconds after which the driver is considered dead if it does not update its C'n'C timestamp.
            /// </summary>
            public long DriverTimeoutMs()
            {
                return Aeron.Context.CheckDebugTimeout(_driverTimeoutMs, TimeUnit.MILLIS, nameof(DriverTimeoutMs));
            }
            
            /// <summary>
            /// The timeout in milliseconds after which the driver is considered dead if it does not update its C'n'C timestamp.
            /// </summary>
            /// <param name="driverTimeout"></param>
            /// <returns></returns>
            public DriverContext DriverTimeoutMs(long driverTimeout)
            {
                _driverTimeoutMs = driverTimeout;
                return this;
            }

            public string AeronDirectoryName()
            {
                return _aeronDirectoryName ??= Aeron.Context.GetAeronDirectoryName();
            }

            public DriverContext AeronDirectoryName(string aeronDirectoryName)
            {
                if (string.IsNullOrWhiteSpace(aeronDirectoryName))
                    throw new ArgumentNullException(nameof(aeronDirectoryName));
                
                _aeronDirectoryName = aeronDirectoryName;
                return this;
            }
        }
    }
}
using System;
using System.IO;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs.Mark;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Container for a service in the cluster managed by the Consensus Module. This is where business logic resides and
    /// loaded via <seealso cref="ClusteredServiceContainer.Configuration.SERVICE_CLASS_NAME_PROP_NAME"/> or
    /// <seealso cref="ClusteredServiceContainer.Context.ClusteredService(IClusteredService)"/>.
    /// </summary>
    public sealed class ClusteredServiceContainer : IDisposable
    {
        /// <summary>
        /// Launch the clustered service container and await a shutdown signal.
        /// </summary>
        /// <param name="args"> command line argument which is a list for properties files as URLs or filenames. </param>
        public static void Main(string[] args)
        {
            using (ClusteredServiceContainer container = Launch())
            {
                container.Ctx().ShutdownSignalBarrier().Await();

                Console.WriteLine("Shutdown ClusteredServiceContainer...");
            }
        }

        private readonly Context ctx;
        private readonly AgentRunner serviceAgentRunner;

        private ClusteredServiceContainer(Context ctx)
        {
            this.ctx = ctx;

            try
            {
                ctx.Conclude();
            }
            catch (Exception)
            {
                if (null != ctx.ClusterMarkFile())
                {
                    ctx.ClusterMarkFile().SignalFailedStart();
                }

                ctx.Dispose();
                throw;
            }

            ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
            serviceAgentRunner = new AgentRunner(ctx.IdleStrategy(), ctx.ErrorHandler(), ctx.ErrorCounter(), agent);
        }

        /// <summary>
        /// Launch an ClusteredServiceContainer using a default configuration.
        /// </summary>
        /// <returns> a new instance of a ClusteredServiceContainer. </returns>
        public static ClusteredServiceContainer Launch()
        {
            return Launch(new Context());
        }

        /// <summary>
        /// Launch a ClusteredServiceContainer by providing a configuration context.
        /// </summary>
        /// <param name="ctx"> for the configuration parameters. </param>
        /// <returns> a new instance of a ClusteredServiceContainer. </returns>
        public static ClusteredServiceContainer Launch(Context ctx)
        {
            ClusteredServiceContainer clusteredServiceContainer = new ClusteredServiceContainer(ctx);
            AgentRunner.StartOnThread(clusteredServiceContainer.serviceAgentRunner,
                clusteredServiceContainer.ctx.ThreadFactory());

            return clusteredServiceContainer;
        }

        /// <summary>
        /// Get the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>.
        /// </summary>
        /// <returns> the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>. </returns>
        public Context Ctx()
        {
            return ctx;
        }

        public void Dispose()
        {
            serviceAgentRunner?.Dispose();
        }

        /// <summary>
        /// Configuration options for the consensus module and service container within a cluster.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Type of snapshot for this service.
            /// </summary>
            public const long SNAPSHOT_TYPE_ID = 2;

            /// <summary>
            /// Update interval for cluster mark file.
            /// </summary>
            public static readonly long MARK_FILE_UPDATE_INTERVAL_NS = 1_000_000_000L;

            /// <summary>
            /// Property name for the identity of the cluster instance.
            /// </summary>
            public const string CLUSTER_ID_PROP_NAME = "aeron.cluster.id";

            /// <summary>
            /// Default identity for a clustered instance.
            /// </summary>
            public const int CLUSTER_ID_DEFAULT = 0;

            /// <summary>
            /// Identity for a clustered service. Services should be numbered from 0 and be contiguous.
            /// </summary>
            public const string SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

            /// <summary>
            /// Default identity for a clustered service.
            /// </summary>
            public const int SERVICE_ID_DEFAULT = 0;

            /// <summary>
            /// Name for a clustered service to be the role of the <seealso cref="IAgent"/>.
            /// </summary>
            public const string SERVICE_NAME_PROP_NAME = "aeron.cluster.service.name";

            /// <summary>
            /// Name for a clustered service to be the role of the <seealso cref="IAgent"/>.
            /// </summary>
            public const string SERVICE_NAME_DEFAULT = "clustered-service";

            /// <summary>
            /// Class name for dynamically loading a <seealso cref="IClusteredService"/>. This is used if
            /// <seealso cref="Context.ClusteredService()"/> is not set.
            /// </summary>
            public const string SERVICE_CLASS_NAME_PROP_NAME = "aeron.cluster.service.class.name";

            /// <summary>
            /// Default channel to be used for log or snapshot replay on startup.
            /// </summary>
            public const string REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

            /// <summary>
            /// Channel to be used for log or snapshot replay on startup.
            /// </summary>
            public static readonly string REPLAY_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

            /// <summary>
            /// Stream id within a channel for the clustered log or snapshot replay.
            /// </summary>
            public const string REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

            /// <summary>
            /// Default stream id for the log or snapshot replay within a channel.
            /// </summary>
            public const int REPLAY_STREAM_ID_DEFAULT = 103;

            /// <summary>
            /// Channel for control communications between the local consensus module and services.
            /// </summary>
            public const string CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.control.channel";

            /// <summary>
            ///  Default channel for communications between the local consensus module and services. This should be IPC.
            /// </summary>
            public const string CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=128k";

            /// <summary>
            /// Stream id within the control channel for communications from the consensus module to the services.
            /// </summary>
            public const string SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

            /// <summary>
            /// Default stream id within the control channel for communications from the consensus module to the services.
            /// </summary>
            public const int SERVICE_STREAM_ID_DEFAULT = 104;

            /// <summary>
            /// Stream id within a channel for communications from the services to the consensus module.
            /// </summary>
            public const string CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";

            /// <summary>
            /// Default stream id within a channel for communications from the services to the consensus module.
            /// </summary>
            public const int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 105;

            /// <summary>
            /// Default channel to be used for archiving snapshots.
            /// </summary>
            public const string SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

            /// <summary>
            /// Channel to be used for archiving snapshots.
            /// </summary>
            public static readonly string SNAPSHOT_CHANNEL_DEFAULT = "aeron:ipc?alias=snapshot";

            /// <summary>
            /// Stream id within a channel for archiving snapshots.
            /// </summary>
            public const string SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

            /// <summary>
            ///  Default stream id for the archived snapshots within a channel.
            /// </summary>
            public const int SNAPSHOT_STREAM_ID_DEFAULT = 106;

            /// <summary>
            /// Directory to use for the aeron cluster.
            /// </summary>
            public const string CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";

            /// <summary>
            /// Default directory to use for the aeron cluster.
            /// </summary>
            public const string CLUSTER_DIR_DEFAULT = "aeron-cluster";

            /// <summary>
            /// Length in bytes of the error buffer for the cluster container.
            /// </summary>
            public const string ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.service.error.buffer.length";

            /// <summary>
            /// Default length in bytes of the error buffer for the cluster container.
            /// </summary>
            public const int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

            /// <summary>
            /// Is this a responding service to client requests property.
            /// </summary>
            public const string RESPONDER_SERVICE_PROP_NAME = "aeron.cluster.service.responder";

            /// <summary>
            /// Default to true that this a responding service to client requests.
            /// </summary>
            public const bool RESPONDER_SERVICE_DEFAULT = true;

            /// <summary>
            /// Fragment limit to use when polling the log.
            /// </summary>
            public const string LOG_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.log.fragment.limit";

            /// <summary>
            /// Default fragment limit for polling log.
            /// </summary>
            public const int LOG_FRAGMENT_LIMIT_DEFAULT = 50;

            /// <summary>
            /// Delegating <seealso cref="ErrorHandler"/> which will be first in the chain before delegating to the
            /// <seealso cref="Context.ErrorHandler()"/>.
            /// </summary>
            public const string DELEGATING_ERROR_HANDLER_PROP_NAME = "aeron.cluster.service.delegating.error.handler";

            /// <summary>
            /// Counter type id for the cluster node role.
            /// </summary>
            public const int CLUSTER_NODE_ROLE_TYPE_ID = AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID;

            /// <summary>
            /// Counter type id of the commit position.
            /// </summary>
            public const int COMMIT_POSITION_TYPE_ID = AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;

            /// <summary>
            /// Counter type id for the clustered service error count.
            /// </summary>
            public const int CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID =
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID;

            /// <summary>
            /// The value <seealso cref="CLUSTER_ID_DEFAULT"/> or system property <seealso cref="CLUSTER_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_ID_DEFAULT"/> or system property <seealso cref="CLUSTER_ID_PROP_NAME"/> if set. </returns>
            public static int ClusterId()
            {
                return Config.GetInteger(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SERVICE_ID_DEFAULT"/> or system property <seealso cref="SERVICE_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SERVICE_ID_DEFAULT"/> or system property <seealso cref="SERVICE_ID_PROP_NAME"/> if set. </returns>
            public static int ServiceId()
            {
                return Config.GetInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SERVICE_NAME_DEFAULT"/> or system property <seealso cref="SERVICE_NAME_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SERVICE_NAME_DEFAULT"/> or system property <seealso cref="SERVICE_NAME_PROP_NAME"/> if set. </returns>
            public static string ServiceName()
            {
                return Config.GetProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="REPLAY_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="REPLAY_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ReplayChannel()
            {
                return Config.GetProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set. </returns>
            public static int ReplayStreamId()
            {
                return Config.GetInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ControlChannel()
            {
                return Config.GetProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ConsensusModuleStreamId()
            {
                return Config.GetInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SERVICE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="SERVICE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SERVICE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="SERVICE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ServiceStreamId()
            {
                return Config.GetInteger(SERVICE_STREAM_ID_PROP_NAME, SERVICE_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="SNAPSHOT_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="SNAPSHOT_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string SnapshotChannel()
            {
                return Config.GetProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="SNAPSHOT_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="SNAPSHOT_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int SnapshotStreamId()
            {
                return Config.GetInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// Default <see cref="IIdleStrategy"/> to be employed for cluster agents.
            /// </summary>
            public const string DEFAULT_IDLE_STRATEGY = "BackoffIdleStrategy";

            /// <summary>
            /// <see cref="IIdleStrategy"/> to be employed for cluster agents.
            /// </summary>
            public const string CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

            /// <summary>
            /// Create a supplier of <seealso cref="IIdleStrategy"/>s that will use the system property.
            /// </summary>
            /// <param name="controllableStatus"> if a <seealso cref="ControllableIdleStrategy"/> is required. </param>
            /// <returns> the new idle strategy </returns>
            public static Func<IIdleStrategy> IdleStrategySupplier(StatusIndicator controllableStatus)
            {
                return () =>
                {
                    var name = Config.GetProperty(CLUSTER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                    return IdleStrategyFactory.Create(name, controllableStatus);
                };
            }

            /// <summary>
            /// The value <seealso cref="CLUSTER_DIR_DEFAULT"/> or system property <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_DIR_DEFAULT"/> or system property <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set. </returns>
            public static string ClusterDirName()
            {
                return Config.GetProperty(CLUSTER_DIR_PROP_NAME, CLUSTER_DIR_DEFAULT);
            }

            /// <summary>
            /// Size in bytes of the error buffer in the mark file.
            /// </summary>
            /// <returns> length of error buffer in bytes. </returns>
            /// <seealso cref="ERROR_BUFFER_LENGTH_PROP_NAME"/>
            public static int ErrorBufferLength()
            {
                return Config.GetSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="RESPONDER_SERVICE_DEFAULT"/> or system property <seealso cref="RESPONDER_SERVICE_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="RESPONDER_SERVICE_DEFAULT"/> or system property <seealso cref="RESPONDER_SERVICE_PROP_NAME"/> if set. </returns>
            public static bool IsRespondingService()
            {
                var property = Config.GetProperty(RESPONDER_SERVICE_PROP_NAME);
                if (null == property)
                {
                    return RESPONDER_SERVICE_DEFAULT;
                }

                return "true".Equals(property);
            }

            /// <summary>
            /// The value <seealso cref="LOG_FRAGMENT_LIMIT_DEFAULT"/> or system property
            /// <seealso cref="LOG_FRAGMENT_LIMIT_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="LOG_FRAGMENT_LIMIT_DEFAULT"/> or system property
            /// <seealso cref="LOG_FRAGMENT_LIMIT_PROP_NAME"/> if set. </returns>
            public static int LogFragmentLimit()
            {
                return Config.GetInteger(LOG_FRAGMENT_LIMIT_PROP_NAME, LOG_FRAGMENT_LIMIT_DEFAULT);
            }

            /// <summary>
            /// Create a new <seealso cref="IClusteredService"/> based on the configured <seealso cref="SERVICE_CLASS_NAME_PROP_NAME"/>.
            /// </summary>
            /// <returns> a new <seealso cref="IClusteredService"/> based on the configured <seealso cref="SERVICE_CLASS_NAME_PROP_NAME"/>. </returns>
            public static IClusteredService NewClusteredService()
            {
                string className = Config.GetProperty(SERVICE_CLASS_NAME_PROP_NAME);
                if (null == className)
                {
                    throw new ClusterException("either a instance or class name for the service must be provided");
                }

                return (IClusteredService)Activator.CreateInstance(Type.GetType(className));
            }

            /// <summary>
            /// Create a new <seealso cref="DelegatingErrorHandler"/> defined by <seealso cref="DELEGATING_ERROR_HANDLER_PROP_NAME"/>.
            /// </summary>
            /// <returns> a new <seealso cref="DelegatingErrorHandler"/> defined by <seealso cref="DELEGATING_ERROR_HANDLER_PROP_NAME"/> or
            /// null if property not set. </returns>
            public static DelegatingErrorHandler NewDelegatingErrorHandler()
            {
                string className = Config.GetProperty(DELEGATING_ERROR_HANDLER_PROP_NAME);
                if (null != className)
                {
                    return (DelegatingErrorHandler)Activator.CreateInstance(Type.GetType(className));
                }

                return null;
            }
        }

        /// <summary>
        /// The context will be owned by <seealso cref="ClusteredServiceAgent"/> after a successful
        /// <seealso cref="ClusteredServiceContainer.Launch(Context)"/> and closed via <seealso cref="ClusteredServiceContainer.Dispose()"/>.
        /// </summary>
        public class Context
        {
            private int _isConcluded = 0;
            private int appVersion = SemanticVersion.Compose(0, 0, 1);
            private int clusterId = Configuration.ClusterId();
            private int serviceId = Configuration.ServiceId();
            private string serviceName = Configuration.ServiceName();
            private string replayChannel = Configuration.ReplayChannel();
            private int replayStreamId = Configuration.ReplayStreamId();
            private string controlChannel = Configuration.ControlChannel();
            private int consensusModuleStreamId = Configuration.ConsensusModuleStreamId();
            private int serviceStreamId = Configuration.ServiceStreamId();
            private string snapshotChannel = Configuration.SnapshotChannel();
            private int snapshotStreamId = Configuration.SnapshotStreamId();
            private int errorBufferLength = Configuration.ErrorBufferLength();
            private bool isRespondingService = Configuration.IsRespondingService();
            private int logFragmentLimit = Configuration.LogFragmentLimit();

            private CountdownEvent abortLatch;
            private IThreadFactory threadFactory;
            private Func<IIdleStrategy> idleStrategySupplier;
            private IEpochClock epochClock;
            private DistinctErrorLog errorLog;
            private ErrorHandler errorHandler;
            private DelegatingErrorHandler delegatingErrorHandler;
            private AtomicCounter errorCounter;
            private CountedErrorHandler countedErrorHandler;
            private AeronArchive.Context archiveContext;
            private string clusterDirectoryName = Configuration.ClusterDirName();
            private DirectoryInfo clusterDir;
            private string aeronDirectoryName = Adaptive.Aeron.Aeron.Context.GetAeronDirectoryName();
            private Aeron.Aeron aeron;
            private bool ownsAeronClient;

            private IClusteredService clusteredService;
            private ShutdownSignalBarrier shutdownSignalBarrier;
            private Action terminationHook;
            private ClusterMarkFile markFile;

            /// <summary>
            /// Perform a shallow copy of the object.
            /// </summary>
            /// <returns> a shallow copy of the object.</returns>
            public Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// Conclude configuration by setting up defaults when specifics are not provided.
            /// </summary>
            public void Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

                if (serviceId < 0 || serviceId > 127)
                {
                    throw new ConfigurationException("service id outside allowed range (0-127): " + serviceId);
                }

                if (null == threadFactory)
                {
                    threadFactory = new DefaultThreadFactory();
                }

                if (null == idleStrategySupplier)
                {
                    idleStrategySupplier = Configuration.IdleStrategySupplier(null);
                }

                if (null == epochClock)
                {
                    epochClock = SystemEpochClock.INSTANCE;
                }

                if (null == clusterDir)
                {
                    clusterDir = new DirectoryInfo(clusterDirectoryName);
                }

                if (!clusterDir.Exists)
                {
                    Directory.CreateDirectory(clusterDir.FullName);
                }

                if (null == markFile)
                {
                    markFile = new ClusterMarkFile(
                        new FileInfo(Path.Combine(clusterDir.FullName,
                            Cluster.ClusterMarkFile.MarkFilenameForService(serviceId))),
                        ClusterComponentType.CONTAINER, errorBufferLength, epochClock, 0);
                }

                if (null == errorLog)
                {
                    errorLog = new DistinctErrorLog(markFile.ErrorBuffer, epochClock); // US_ASCII
                }

                errorHandler = Adaptive.Aeron.Aeron.Context.SetupErrorHandler(errorHandler, errorLog);

                if (null == delegatingErrorHandler)
                {
                    delegatingErrorHandler = Configuration.NewDelegatingErrorHandler();
                    if (null != delegatingErrorHandler)
                    {
                        delegatingErrorHandler.Next(errorHandler);
                        errorHandler = delegatingErrorHandler.OnError;
                    }
                }
                else
                {
                    delegatingErrorHandler.Next(errorHandler);
                    errorHandler = delegatingErrorHandler.OnError;
                }

                if (null == aeron)
                {
                    aeron = Adaptive.Aeron.Aeron.Connect(
                        new Aeron.Aeron.Context()
                            .AeronDirectoryName(aeronDirectoryName)
                            .ErrorHandler(errorHandler)
                            .SubscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                            .AwaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                            .EpochClock(epochClock));

                    ownsAeronClient = true;
                }

                if (aeron.Ctx.SubscriberErrorHandler() != RethrowingErrorHandler.INSTANCE)
                {
                    throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
                }

                if (null == errorCounter)
                {
                    String label = "Cluster Container Errors - clusterId=" + clusterId + " serviceId=" + serviceId;
                    errorCounter = aeron.AddCounter(Configuration.CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID, label);
                }

                if (null == countedErrorHandler)
                {
                    countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                    if (ownsAeronClient)
                    {
                        aeron.Ctx.ErrorHandler(countedErrorHandler.OnError);
                    }
                }

                if (null == archiveContext)
                {
                    archiveContext = new AeronArchive.Context()
                        .ControlRequestChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlResponseChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlRequestStreamId(AeronArchive.Configuration.LocalControlStreamId());
                }
                
                if (!archiveContext.ControlRequestChannel().StartsWith(Adaptive.Aeron.Aeron.Context.IPC_CHANNEL))
                {
                    throw new ClusterException("local archive control must be IPC");
                }

                if (!archiveContext.ControlResponseChannel().StartsWith(Adaptive.Aeron.Aeron.Context.IPC_CHANNEL))
                {
                    throw new ClusterException("local archive control must be IPC");
                }

                archiveContext
                    .AeronClient(aeron)
                    .OwnsAeronClient(false)
                    .Lock(NoOpLock.Instance)
                    .ErrorHandler(countedErrorHandler.OnError);

                if (null == shutdownSignalBarrier)
                {
                    shutdownSignalBarrier = new ShutdownSignalBarrier();
                }

                if (null == terminationHook)
                {
                    terminationHook = () => shutdownSignalBarrier.Signal();
                }

                if (null == clusteredService)
                {
                    clusteredService = Configuration.NewClusteredService();
                }

                abortLatch = new CountdownEvent(aeron.ConductorAgentInvoker == null ? 1 : 0);
                ConcludeMarkFile();
            }

            /// <summary>
            /// User assigned application version which appended to the log as the appVersion in new leadership events.
            /// <para>
            /// This can be validated using <seealso cref="SemanticVersion"/> to ensure only application nodes of the same
            /// major version communicate with each other.
            ///         
            /// </para>
            /// </summary>
            /// <param name="appVersion"> for user application. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AppVersion(int appVersion)
            {
                this.appVersion = appVersion;
                return this;
            }

            /// <summary>
            /// User assigned application version which appended to the log as the appVersion in new leadership events.
            /// <para>
            /// This can be validated using <seealso cref="SemanticVersion"/> to ensure only application nodes of the same
            /// major version communicate with each other.
            /// 
            /// </para>
            /// </summary>
            /// <returns> appVersion for user application. </returns>
            public int AppVersion()
            {
                return appVersion;
            }

            /// <summary>
            /// Set the id for this cluster instance. This must match with the Consensus Module.
            /// </summary>
            /// <param name="clusterId"> for this clustered instance. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CLUSTER_ID_PROP_NAME"></seealso>
            public Context ClusterId(int clusterId)
            {
                this.clusterId = clusterId;
                return this;
            }

            /// <summary>
            /// Get the id for this cluster instance. This must match with the Consensus Module.
            /// </summary>
            /// <returns> the id for this cluster instance. </returns>
            /// <seealso cref="Configuration.CLUSTER_ID_PROP_NAME"></seealso>
            public int ClusterId()
            {
                return clusterId;
            }

            /// <summary>
            /// Set the id for this clustered service. Services should be numbered from 0 and be contiguous.
            /// </summary>
            /// <param name="serviceId"> for this clustered service. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SERVICE_ID_PROP_NAME"></seealso>
            public Context ServiceId(int serviceId)
            {
                this.serviceId = serviceId;
                return this;
            }

            /// <summary>
            /// Get the id for this clustered service. Services should be numbered from 0 and be contiguous.
            /// </summary>
            /// <returns> the id for this clustered service. </returns>
            /// <seealso cref="Configuration.SERVICE_ID_PROP_NAME"></seealso>
            public int ServiceId()
            {
                return serviceId;
            }

            /// <summary>
            /// Set the name for a clustered service to be the <see cref="IAgent.RoleName"/> for the <seealso cref="IAgent"/>.
            /// </summary>
            /// <param name="serviceName"> for a clustered service to be the role for the <seealso cref="IAgent"/>. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.SERVICE_NAME_PROP_NAME"></seealso>
            public Context ServiceName(string serviceName)
            {
                this.serviceName = serviceName;
                return this;
            }

            /// <summary>
            /// Get the name for a clustered service to be the <see cref="IAgent.RoleName"/> for the <seealso cref="IAgent"/>.
            /// </summary>
            /// <returns> the name for a clustered service to be the role of the <seealso cref="IAgent"/>. </returns>
            /// <seealso cref="Configuration.SERVICE_NAME_PROP_NAME"></seealso>
            public string ServiceName()
            {
                return serviceName;
            }


            /// <summary>
            /// Set the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="channel"> parameter for the cluster log replay channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.REPLAY_CHANNEL_PROP_NAME"></seealso>
            public Context ReplayChannel(string channel)
            {
                replayChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the channel parameter for the cluster replay channel. </returns>
            /// <seealso cref="Configuration.REPLAY_CHANNEL_PROP_NAME"></seealso>
            public string ReplayChannel()
            {
                return replayChannel;
            }

            /// <summary>
            /// Set the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="streamId"> for the cluster log replay channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.REPLAY_STREAM_ID_PROP_NAME"></seealso>
            public Context ReplayStreamId(int streamId)
            {
                replayStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the stream id for the cluster log replay channel. </returns>
            /// <seealso cref="Configuration.REPLAY_STREAM_ID_PROP_NAME"></seealso>
            public int ReplayStreamId()
            {
                return replayStreamId;
            }

            /// <summary>
            /// Set the channel parameter for bidirectional communications between the consensus module and services.
            /// </summary>
            /// <param name="channel"> parameter for sending messages to the Consensus Module. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public Context ControlChannel(string channel)
            {
                controlChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for bidirectional communications between the consensus module and services.
            /// </summary>
            /// <returns> the channel parameter for sending messages to the Consensus Module. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public string ControlChannel()
            {
                return controlChannel;
            }

            /// <summary>
            /// Set the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <param name="streamId"> for communications from the consensus module and to the services. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SERVICE_STREAM_ID_PROP_NAME"></seealso>
            public Context ServiceStreamId(int streamId)
            {
                serviceStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <returns> the stream id for communications from the consensus module and to the services. </returns>
            /// <seealso cref="Configuration.SERVICE_STREAM_ID_PROP_NAME"></seealso>
            public int ServiceStreamId()
            {
                return serviceStreamId;
            }

            /// <summary>
            /// Set the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <param name="streamId"> for communications from the services to the consensus module. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CONSENSUS_MODULE_STREAM_ID_PROP_NAME"></seealso>
            public Context ConsensusModuleStreamId(int streamId)
            {
                consensusModuleStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <returns> the stream id for communications from the services to the consensus module. </returns>
            /// <seealso cref="Configuration.CONSENSUS_MODULE_STREAM_ID_PROP_NAME"></seealso>
            public int ConsensusModuleStreamId()
            {
                return consensusModuleStreamId;
            }

            /// <summary>
            /// Set the channel parameter for snapshot recordings.
            /// </summary>
            /// <param name="channel"> parameter for snapshot recordings </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_CHANNEL_PROP_NAME"></seealso>
            public Context SnapshotChannel(string channel)
            {
                snapshotChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for snapshot recordings.
            /// </summary>
            /// <returns> the channel parameter for snapshot recordings. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_CHANNEL_PROP_NAME"></seealso>
            public string SnapshotChannel()
            {
                return snapshotChannel;
            }

            /// <summary>
            /// Set the stream id for snapshot recordings.
            /// </summary>
            /// <param name="streamId"> for snapshot recordings. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SNAPSHOT_STREAM_ID_PROP_NAME"></seealso>
            public Context SnapshotStreamId(int streamId)
            {
                snapshotStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for snapshot recordings.
            /// </summary>
            /// <returns> the stream id for snapshot recordings. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_STREAM_ID_PROP_NAME"></seealso>
            public int SnapshotStreamId()
            {
                return snapshotStreamId;
            }

            /// <summary>
            /// Set if this a service that responds to client requests.
            /// </summary>
            /// <param name="isRespondingService"> true if this service responds to client requests, otherwise false. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.RESPONDER_SERVICE_PROP_NAME"></seealso>
            public Context IsRespondingService(bool isRespondingService)
            {
                this.isRespondingService = isRespondingService;
                return this;
            }
            
            /// <summary>
            /// Set the fragment limit to be used when polling the log <seealso cref="Subscription"/>.
            /// </summary>
            /// <param name="logFragmentLimit"> for this clustered service. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.LOG_FRAGMENT_LIMIT_DEFAULT"/>
            public Context LogFragmentLimit(int logFragmentLimit)
            {
                this.logFragmentLimit = logFragmentLimit;
                return this;
            }

            /// <summary>
            /// Get the fragment limit to be used when polling the log <seealso cref="Subscription"/>.
            /// </summary>
            /// <returns> the fragment limit to be used when polling the log <seealso cref="Subscription"/>. </returns>
            /// <seealso cref="Configuration.LOG_FRAGMENT_LIMIT_DEFAULT"/>
            public int LogFragmentLimit()
            {
                return logFragmentLimit;
            }

            /// <summary>
            /// Is this a service that responds to client requests?
            /// </summary>
            /// <returns> true if this service responds to client requests, otherwise false. </returns>
            /// <seealso cref="Configuration.RESPONDER_SERVICE_PROP_NAME"></seealso>
            public bool IsRespondingService()
            {
                return isRespondingService;
            }

            /// <summary>
            /// Get the thread factory used for creating threads.
            /// </summary>
            /// <returns> thread factory used for creating threads. </returns>
            public IThreadFactory ThreadFactory()
            {
                return threadFactory;
            }

            /// <summary>
            /// Set the thread factory used for creating threads.
            /// </summary>
            /// <param name="threadFactory"> used for creating threads </param>
            /// <returns> this for a fluent API. </returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                this.threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// Provides an <seealso cref="IIdleStrategy"/> supplier for the idle strategy for the agent duty cycle.
            /// </summary>
            /// <param name="idleStrategySupplier"> supplier for the idle strategy for the agent duty cycle. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategySupplier(Func<IIdleStrategy> idleStrategySupplier)
            {
                this.idleStrategySupplier = idleStrategySupplier;
                return this;
            }

            /// <summary>
            /// Get a new <seealso cref="IdleStrategy"/> based on configured supplier.
            /// </summary>
            /// <returns> a new <seealso cref="IdleStrategy"/> based on configured supplier. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return idleStrategySupplier();
            }

            /// <summary>
            /// Set the <seealso cref="IEpochClock"/> to be used for tracking wall clock time when interacting with the container.
            /// </summary>
            /// <param name="clock"> <seealso cref="IEpochClock"/> to be used for tracking wall clock time when interacting with the container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EpochClock(IEpochClock clock)
            {
                this.epochClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IEpochClock"/> to used for tracking wall clock time within the container.
            /// </summary>
            /// <returns> the <seealso cref="IEpochClock"/> to used for tracking wall clock time within the container. </returns>
            public IEpochClock EpochClock()
            {
                return epochClock;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.ErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/>.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.ErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/>. </returns>
            public ErrorHandler ErrorHandler()
            {
                return errorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.ErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/>.
            /// </summary>
            /// <param name="errorHandler"> the error handler to be used by the <seealso cref="ClusteredServiceContainer"/>. </param>
            /// <returns> this for a fluent API </returns>
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                this.errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/> which will
            /// delegate to <seealso cref="ErrorHandler()"/> as next in the chain.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/>. </returns>
            /// <seealso cref="Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME"></seealso>
            public DelegatingErrorHandler DelegatingErrorHandler()
            {
                return delegatingErrorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the <seealso cref="ClusteredServiceContainer"/> which will
            /// delegate to <seealso cref="ErrorHandler()"/> as next in the chain.
            /// </summary>
            /// <param name="delegatingErrorHandler"> the error handler to be used by the <seealso cref="ClusteredServiceContainer"/>. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME"> </seealso>
            public Context DelegatingErrorHandler(DelegatingErrorHandler delegatingErrorHandler)
            {
                this.delegatingErrorHandler = delegatingErrorHandler;
                return this;
            }

            /// <summary>
            /// Get the error counter that will record the number of errors the container has observed.
            /// </summary>
            /// <returns> the error counter that will record the number of errors the container has observed. </returns>
            public AtomicCounter ErrorCounter()
            {
                return errorCounter;
            }


            /// <summary>
            /// Set the error counter that will record the number of errors the cluster node has observed.
            /// </summary>
            /// <param name="errorCounter"> the error counter that will record the number of errors the cluster node has observed. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorCounter(AtomicCounter errorCounter)
            {
                this.errorCounter = errorCounter;
                return this;
            }

            /// <summary>
            /// Non-default for context.
            /// </summary>
            /// <param name="countedErrorHandler"> to override the default. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CountedErrorHandler(CountedErrorHandler countedErrorHandler)
            {
                this.countedErrorHandler = countedErrorHandler;
                return this;
            }

            /// <summary>
            /// The <seealso cref="ErrorHandler()"/> that will increment <seealso cref="ErrorCounter()"/> by default.
            /// </summary>
            /// <returns> <seealso cref="ErrorHandler()"/> that will increment <seealso cref="ErrorCounter()"/> by default. </returns>
            public CountedErrorHandler CountedErrorHandler()
            {
                return countedErrorHandler;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <param name="aeronDirectoryName"> the top level Aeron directory. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                this.aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return aeronDirectoryName;
            }

            /// <summary>
            /// An <seealso cref="Adaptive.Aeron.Aeron"/> client for the container.
            /// </summary>
            /// <returns> <seealso cref="Adaptive.Aeron.Aeron"/> client for the container </returns>
            public Aeron.Aeron Aeron()
            {
                return aeron;
            }

            /// <summary>
            /// Provide an <seealso cref="Adaptive.Aeron.Aeron"/> client for the container
            /// <para>
            /// If not provided then one will be created.
            /// 
            /// </para>
            /// </summary>
            /// <param name="aeron"> client for the container </param>
            /// <returns> this for a fluent API. </returns>
            public Context Aeron(Aeron.Aeron aeron)
            {
                this.aeron = aeron;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="Aeron()"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this.ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it? </returns>
            public bool OwnsAeronClient()
            {
                return ownsAeronClient;
            }

            /// <summary>
            /// The service this container holds.
            /// </summary>
            /// <returns> service this container holds. </returns>
            public IClusteredService ClusteredService()
            {
                return clusteredService;
            }

            /// <summary>
            /// Set the service this container is to hold.
            /// </summary>
            /// <param name="clusteredService"> this container is to hold. </param>
            /// <returns> this for fluent API. </returns>
            public Context ClusteredService(IClusteredService clusteredService)
            {
                this.clusteredService = clusteredService;
                return this;
            }

            /// <summary>
            /// Set the context that should be used for communicating with the local Archive.
            /// </summary>
            /// <param name="archiveContext"> that should be used for communicating with the local Archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ArchiveContext(AeronArchive.Context archiveContext)
            {
                this.archiveContext = archiveContext;
                return this;
            }

            /// <summary>
            /// Get the context that should be used for communicating with the local Archive.
            /// </summary>
            /// <returns> the context that should be used for communicating with the local Archive. </returns>
            public AeronArchive.Context ArchiveContext()
            {
                return archiveContext;
            }

            /// <summary>
            /// Set the directory name to use for the consensus module directory..
            /// </summary>
            /// <param name="clusterDirectoryName"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"/>
            public Context ClusterDirectoryName(string clusterDirectoryName)
            {
                this.clusterDirectoryName = clusterDirectoryName;
                return this;
            }

            /// <summary>
            /// The directory name to use for the cluster directory.
            /// </summary>
            /// <returns> directory name for the cluster directory. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"/>
            public string ClusterDirectoryName()
            {
                return clusterDirectoryName;
            }

            /// <summary>
            /// Set the directory to use for the cluster directory.
            /// </summary>
            /// <param name="clusterDir"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"></seealso>
            public Context ClusterDir(DirectoryInfo clusterDir)
            {
                this.clusterDir = clusterDir;
                return this;
            }

            /// <summary>
            /// The directory used for the cluster directory.
            /// </summary>
            /// <returns>  directory for the cluster directory. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"></seealso>
            public DirectoryInfo ClusterDir()
            {
                return clusterDir;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shut down a clustered service.
            /// </summary>
            /// <param name="barrier"> that can be used to shut down a clustered service. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ShutdownSignalBarrier(ShutdownSignalBarrier barrier)
            {
                shutdownSignalBarrier = barrier;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shut down a clustered service.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shut down a clustered service. </returns>
            public ShutdownSignalBarrier ShutdownSignalBarrier()
            {
                return shutdownSignalBarrier;
            }

            /// <summary>
            /// Set the <seealso cref="Action"/> that is called when container is instructed to terminate.
            /// </summary>
            /// <param name="terminationHook"> that can be used to terminate a service container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context TerminationHook(Action terminationHook)
            {
                this.terminationHook = terminationHook;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Action"/> that is called when container is instructed to terminate.
            /// <para>
            /// The default action is to call signal on the <seealso cref="ShutdownSignalBarrier()"/>.
            /// 
            /// </para>
            /// </summary>
            /// <returns> the <seealso cref="Action"/> that can be used to terminate a service container. </returns>
            public Action TerminationHook()
            {
                return terminationHook;
            }

            /// <summary>
            /// Set the <seealso cref="Cluster.ClusterMarkFile"/> in use.
            /// </summary>
            /// <param name="cncFile"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ClusterMarkFile(ClusterMarkFile cncFile)
            {
                this.markFile = cncFile;
                return this;
            }

            /// <summary>
            /// The <seealso cref="Cluster.ClusterMarkFile"/> in use.
            /// </summary>
            /// <returns> CnC file in use. </returns>
            public ClusterMarkFile ClusterMarkFile()
            {
                return markFile;
            }

            /// <summary>
            /// Set the error buffer length in bytes to use.
            /// </summary>
            /// <param name="errorBufferLength"> in bytes to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorBufferLength(int errorBufferLength)
            {
                this.errorBufferLength = errorBufferLength;
                return this;
            }

            /// <summary>
            /// The error buffer length in bytes.
            /// </summary>
            /// <returns> error buffer length in bytes. </returns>
            public int ErrorBufferLength()
            {
                return errorBufferLength;
            }

            /// <summary>
            /// Set the <seealso cref="DistinctErrorLog"/> in use.
            /// </summary>
            /// <param name="errorLog"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorLog(DistinctErrorLog errorLog)
            {
                this.errorLog = errorLog;
                return this;
            }

            /// <summary>
            /// The <seealso cref="DistinctErrorLog"/> in use.
            /// </summary>
            /// <returns> <seealso cref="DistinctErrorLog"/> in use. </returns>
            public DistinctErrorLog ErrorLog()
            {
                return errorLog;
            }

            /// <summary>
            /// Delete the cluster container directory.
            /// </summary>
            public void DeleteDirectory()
            {
                if (null != clusterDir)
                {
                    IoUtil.Delete(clusterDir, false);
                }
            }

            /// <summary>
            /// Close the context and free applicable resources.
            /// <para>
            /// If <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="Aeron()"/> client will be closed.
            /// </para>
            /// </summary>
            public void Dispose()
            {
                ErrorHandler errorHandler = CountedErrorHandler().OnError;
                if (ownsAeronClient)
                {
                    CloseHelper.Dispose(errorHandler, aeron);
                }

                CloseHelper.Dispose(errorHandler, markFile);
            }

            internal CountdownEvent AbortLatch()
            {
                return abortLatch;
            }

            private void ConcludeMarkFile()
            {
                Cluster.ClusterMarkFile.CheckHeaderLength(
                    aeron.Ctx.AeronDirectoryName(),
                    ControlChannel(),
                    null,
                    serviceName,
                    null);

                var encoder = markFile.Encoder();

                encoder
                    .ArchiveStreamId(archiveContext.ControlRequestStreamId())
                    .ServiceStreamId(serviceStreamId)
                    .ConsensusModuleStreamId(consensusModuleStreamId)
                    .IngressStreamId(Adaptive.Aeron.Aeron.NULL_VALUE)
                    .MemberId(Adaptive.Aeron.Aeron.NULL_VALUE)
                    .ServiceId(serviceId)
                    .ClusterId(clusterId)
                    .AeronDirectory(aeron.Ctx.AeronDirectoryName())
                    .ControlChannel(controlChannel)
                    .IngressChannel(string.Empty)
                    .ServiceName(serviceName)
                    .Authenticator(string.Empty);

                markFile.UpdateActivityTimestamp(epochClock.Time());
                markFile.SignalReady();
            }
            
            /// <summary>
            /// {@inheritDoc}
            /// </summary>
            public override string ToString()
            {
                return "ClusteredServiceContainer.Context" +
                       "\n{" +
                       "\n    isConcluded=" + (1 == _isConcluded) +
                       "\n    ownsAeronClient=" + ownsAeronClient +
                       "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                       "\n    aeron=" + aeron +
                       "\n    archiveContext=" + archiveContext +
                       "\n    clusterDirectoryName='" + clusterDirectoryName + '\'' +
                       "\n    clusterDir=" + clusterDir +
                       "\n    appVersion=" + appVersion +
                       "\n    clusterId=" + clusterId +
                       "\n    serviceId=" + serviceId +
                       "\n    serviceName='" + serviceName + '\'' +
                       "\n    replayChannel='" + replayChannel + '\'' +
                       "\n    replayStreamId=" + replayStreamId +
                       "\n    controlChannel='" + controlChannel + '\'' +
                       "\n    consensusModuleStreamId=" + consensusModuleStreamId +
                       "\n    serviceStreamId=" + serviceStreamId +
                       "\n    snapshotChannel='" + snapshotChannel + '\'' +
                       "\n    snapshotStreamId=" + snapshotStreamId +
                       "\n    errorBufferLength=" + errorBufferLength +
                       "\n    isRespondingService=" + isRespondingService +
                       "\n    logFragmentLimit=" + logFragmentLimit +
                       "\n    abortLatch=" + abortLatch +
                       "\n    threadFactory=" + threadFactory +
                       "\n    idleStrategySupplier=" + idleStrategySupplier +
                       "\n    epochClock=" + epochClock +
                       "\n    errorLog=" + errorLog +
                       "\n    errorHandler=" + errorHandler +
                       "\n    delegatingErrorHandler=" + delegatingErrorHandler +
                       "\n    errorCounter=" + errorCounter +
                       "\n    countedErrorHandler=" + countedErrorHandler +
                       "\n    clusteredService=" + clusteredService +
                       "\n    shutdownSignalBarrier=" + shutdownSignalBarrier +
                       "\n    terminationHook=" + terminationHook +
                       "\n    markFile=" + markFile +
                       "\n}";
            }

        }
    }
}
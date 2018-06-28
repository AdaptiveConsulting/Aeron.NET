using System;
using System.IO;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using Adaptive.Cluster.Codecs.Mark;

namespace Adaptive.Cluster.Service
{
    public sealed class ClusteredServiceContainer : IDisposable
    {
        public const int SYSTEM_COUNTER_TYPE_ID = 0;

        /// <summary>
        /// Type of snapshot for this service.
        /// </summary>
        public const long SNAPSHOT_TYPE_ID = 2;

        private readonly Context ctx;
        private readonly AgentRunner serviceAgentRunner;

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

        private ClusteredServiceContainer(Context ctx)
        {
            this.ctx = ctx;
            ctx.Conclude();

            ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
            serviceAgentRunner = new AgentRunner(ctx.IdleStrategy(), ctx.ErrorHandler(), ctx.ErrorCounter(), agent);
        }

        private ClusteredServiceContainer Start()
        {
            AgentRunner.StartOnThread(serviceAgentRunner, ctx.ThreadFactory());
            return this;
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
            return (new ClusteredServiceContainer(ctx)).Start();
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
            ctx?.Dispose();
        }

        /// <summary>
        /// Configuration options for the consensus module and service container within a cluster.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Identity for a clustered service.
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
            /// Channel for communications between the local consensus module and services.
            /// </summary>
            public const string SERVICE_CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.service.control.channel";

            /// <summary>
            ///  Default channel for communications between the local consensus module and services. This should be IPC.
            /// </summary>
            public const string SERVICE_CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=64k|mtu=8k";

            /// <summary>
            /// Stream id within a channel for communications from the consensus module to the services.
            /// </summary>
            public const string SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

            /// <summary>
            /// Default stream id within a channel for communications from the consensus module to the services.
            /// </summary>
            public const int SERVICE_CONTROL_STREAM_ID_DEFAULT = 104;

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
            public static readonly string SNAPSHOT_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

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
            /// The value <seealso cref="#SERVICE_ID_DEFAULT"/> or system property <seealso cref="#SERVICE_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SERVICE_ID_DEFAULT"/> or system property <seealso cref="#SERVICE_ID_PROP_NAME"/> if set. </returns>
            public static int ServiceId()
            {
                return Config.GetInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SERVICE_NAME_DEFAULT"/> or system property <seealso cref="#SERVICE_NAME_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SERVICE_NAME_DEFAULT"/> or system property <seealso cref="#SERVICE_NAME_PROP_NAME"/> if set. </returns>
            public static string ServiceName()
            {
                return Config.GetProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="#REPLAY_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="#REPLAY_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ReplayChannel()
            {
                return Config.GetProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="#REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="#REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="#REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set. </returns>
            public static int ReplayStreamId()
            {
                return Config.GetInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SERVICE_CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="#SERVICE_CONTROL_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SERVICE_CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="#SERVICE_CONTROL_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ServiceControlChannel()
            {
                return Config.GetProperty(SERVICE_CONTROL_CHANNEL_PROP_NAME, SERVICE_CONTROL_CHANNEL_DEFAULT);
            }
            
            /// <summary>
            /// The value <seealso cref="#CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ConsensusModuleStreamId()
            {
                return Config.GetInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SERVICE_CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#SERVICE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SERVICE_CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#SERVICE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ServiceStreamId()
            {
                return Config.GetInteger(SERVICE_STREAM_ID_PROP_NAME, SERVICE_CONTROL_STREAM_ID_DEFAULT);
            }
            
            /// <summary>
            /// The value <seealso cref="#SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string SnapshotChannel()
            {
                return Config.GetProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="#SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int SnapshotStreamId()
            {
                return Config.GetInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
            }

            public const string DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";
            public const string CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

            /// <summary>
            /// Create a supplier of <seealso cref="IdleStrategy"/>s that will use the system property.
            /// </summary>
            /// <param name="controllableStatus"> if a <seealso cref="org.agrona.concurrent.ControllableIdleStrategy"/> is required. </param>
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
            /// The value <seealso cref="#CLUSTER_DIR_DEFAULT"/> or system property <seealso cref="#CLUSTER_DIR_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#CLUSTER_DIR_DEFAULT"/> or system property <seealso cref="#CLUSTER_DIR_PROP_NAME"/> if set. </returns>
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
        }

        public class Context : IDisposable
        {
            private int serviceId = Configuration.ServiceId();
            private string serviceName = Configuration.ServiceName();
            private string replayChannel = Configuration.ReplayChannel();
            private int replayStreamId = Configuration.ReplayStreamId();
            private string serviceControlChannel = Configuration.ServiceControlChannel();
            private int consensusModuleStreamId = Configuration.ConsensusModuleStreamId();
            private int serviceStreamId = Configuration.ServiceStreamId();
            private string snapshotChannel = Configuration.SnapshotChannel();
            private int snapshotStreamId = Configuration.SnapshotStreamId();
            private int errorBufferLength = Configuration.ErrorBufferLength();
            
            private IThreadFactory threadFactory;
            private Func<IIdleStrategy> idleStrategySupplier;
            private IEpochClock epochClock;
            private DistinctErrorLog errorLog;
            private ErrorHandler errorHandler;
            private AtomicCounter errorCounter;
            private CountedErrorHandler countedErrorHandler;
            private AeronArchive.Context archiveContext;
            private string clusteredServiceDirectoryName = Configuration.ClusterDirName();
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
                return (Context) MemberwiseClone();
            }

            public void Conclude()
            {
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
                    epochClock = new SystemEpochClock();
                }

                if (null == clusterDir)
                {
                    clusterDir = new DirectoryInfo(clusteredServiceDirectoryName);
                }

                if (!clusterDir.Exists)
                {
                    Directory.CreateDirectory(clusterDir.FullName);
                }

                if (null == markFile)
                {
                    markFile = new ClusterMarkFile(
                        new FileInfo(Path.Combine(clusterDir.FullName, ClusterMarkFile.MarkFilenameForService(serviceId))),
                        ClusterComponentType.CONTAINER,
                        errorBufferLength,
                        epochClock,
                        0);
                }

                if (null == errorLog)
                {
                    errorLog = new DistinctErrorLog(markFile.ErrorBuffer, epochClock);
                }

                if (null == errorHandler)
                {
                    errorHandler = new LoggingErrorHandler(errorLog).OnError; // TODO Use interface
                }

                if (null == aeron)
                {
                    aeron = Adaptive.Aeron.Aeron.Connect(
                        new Aeron.Aeron.Context()
                            .AeronDirectoryName(aeronDirectoryName)
                            .ErrorHandler(errorHandler)
                            .EpochClock(epochClock));

                    ownsAeronClient = true;
                }

                if (null == errorCounter)
                {
                    errorCounter = aeron.AddCounter(SYSTEM_COUNTER_TYPE_ID, "Cluster errors - service " + serviceId);
                }

                if (null == countedErrorHandler)
                {
                    countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                    if (ownsAeronClient)
                    {
                        aeron.Ctx().ErrorHandler(countedErrorHandler.OnError);
                    }
                }

                if (null == archiveContext)
                {
                    archiveContext = new AeronArchive.Context()
                        .ControlRequestChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlResponseChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlRequestStreamId(AeronArchive.Configuration.LocalControlStreamId());
                }

                archiveContext
                    .AeronClient(aeron)
                    .OwnsAeronClient(false)
                    .Lock(new NoOpLock());

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
                    string className = Config.GetProperty(Configuration.SERVICE_CLASS_NAME_PROP_NAME);
                    if (null == className)
                    {
                        throw new ClusterException("either a ClusteredService instance or class name for the service must be provided");
                    }

                    clusteredService = (IClusteredService) Activator.CreateInstance(Type.GetType(className));
                }

                ConcludeMarkFile();
            }

            /// <summary>
            /// Set the id for this clustered service.
            /// </summary>
            /// <param name="serviceId"> for this clustered service. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME </seealso>
            public Context ServiceId(int serviceId)
            {
                this.serviceId = serviceId;
                return this;
            }

            /// <summary>
            /// Get the id for this clustered service.
            /// </summary>
            /// <returns> the id for this clustered service. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME </seealso>
            public int ServiceId()
            {
                return serviceId;
            }

            /// <summary>
            /// Set the name for a clustered service to be the role of the <seealso cref="IAgent"/>.
            /// </summary>
            /// <param name="serviceName"> for a clustered service to be the role of the <seealso cref="IAgent"/>. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.SERVICE_NAME_PROP_NAME"></seealso>
            public Context ServiceName(string serviceName)
            {
                this.serviceName = serviceName;
                return this;
            }

            /// <summary>
            /// Get the name for a clustered service to be the role of the <seealso cref="IAgent"/>.
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
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME </seealso>
            public Context ReplayChannel(string channel)
            {
                replayChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the channel parameter for the cluster replay channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME </seealso>
            public string ReplayChannel()
            {
                return replayChannel;
            }

            /// <summary>
            /// Set the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="streamId"> for the cluster log replay channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME </seealso>
            public Context ReplayStreamId(int streamId)
            {
                replayStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the stream id for the cluster log replay channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME </seealso>
            public int ReplayStreamId()
            {
                return replayStreamId;
            }

            /// <summary>
            /// Set the channel parameter for sending messages to the Consensus Module.
            /// </summary>
            /// <param name="channel"> parameter for sending messages to the Consensus Module. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME </seealso>
            public Context ServiceControlChannel(string channel)
            {
                serviceControlChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for sending messages to the Consensus Module.
            /// </summary>
            /// <returns> the channel parameter for sending messages to the Consensus Module. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME </seealso>
            public string ServiceControlChannel()
            {
                return serviceControlChannel;
            }

            /// <summary>
            /// Set the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <param name="streamId"> for communications from the consensus module and to the services. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= Configuration#SERVICE_STREAM_ID_PROP_NAME </seealso>
            public Context ServiceStreamId(int streamId)
            {
                serviceStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <returns> the stream id for communications from the consensus module and to the services. </returns>
            /// <seealso cref= Configuration#SERVICE_STREAM_ID_PROP_NAME </seealso>
            public int ServiceStreamId()
            {
                return serviceStreamId;
            }

            /// <summary>
            /// Set the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <param name="streamId"> for communications from the services to the consensus module. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME </seealso>
            public Context ConsensusModuleStreamId(int streamId)
            {
                consensusModuleStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <returns> the stream id for communications from the services to the consensus module. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME </seealso>
            public int ConsensusModuleStreamId()
            {
                return consensusModuleStreamId;
            }
           
            /// <summary>
            /// Set the channel parameter for snapshot recordings.
            /// </summary>
            /// <param name="channel"> parameter for snapshot recordings </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_CHANNEL_PROP_NAME </seealso>
            public Context SnapshotChannel(string channel)
            {
                snapshotChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for snapshot recordings.
            /// </summary>
            /// <returns> the channel parameter for snapshot recordings. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_CHANNEL_PROP_NAME </seealso>
            public string SnapshotChannel()
            {
                return snapshotChannel;
            }

            /// <summary>
            /// Set the stream id for snapshot recordings.
            /// </summary>
            /// <param name="streamId"> for snapshot recordings. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= Configuration#SNAPSHOT_STREAM_ID_PROP_NAME </seealso>
            public Context SnapshotStreamId(int streamId)
            {
                snapshotStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for snapshot recordings.
            /// </summary>
            /// <returns> the stream id for snapshot recordings. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_STREAM_ID_PROP_NAME </seealso>
            public int SnapshotStreamId()
            {
                return snapshotStreamId;
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
            /// Provides an <seealso cref="IdleStrategy"/> supplier for the thread responsible for publication/subscription backoff.
            /// </summary>
            /// <param name="idleStrategySupplier"> supplier of thread idle strategy for publication/subscription backoff. </param>
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
            /// Set the <seealso cref="EpochClock"/> to be used for tracking wall clock time when interacting with the archive.
            /// </summary>
            /// <param name="clock"> <seealso cref="EpochClock"/> to be used for tracking wall clock time when interacting with the archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EpochClock(IEpochClock clock)
            {
                this.epochClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="EpochClock"/> to used for tracking wall clock time within the archive.
            /// </summary>
            /// <returns> the <seealso cref="EpochClock"/> to used for tracking wall clock time within the archive. </returns>
            public IEpochClock EpochClock()
            {
                return epochClock;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive. </returns>
            public ErrorHandler ErrorHandler()
            {
                return errorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive.
            /// </summary>
            /// <param name="errorHandler"> the error handler to be used by the Archive. </param>
            /// <returns> this for a fluent API </returns>
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                this.errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the error counter that will record the number of errors the archive has observed.
            /// </summary>
            /// <returns> the error counter that will record the number of errors the archive has observed. </returns>
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
            /// The <seealso cref="#errorHandler()"/> that will increment <seealso cref="#errorCounter()"/> by default.
            /// </summary>
            /// <returns> <seealso cref="#errorHandler()"/> that will increment <seealso cref="#errorCounter()"/> by default. </returns>
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
            /// Does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="#aeron()"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this.ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it? </returns>
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
            /// Set the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive.
            /// </summary>
            /// <param name="archiveContext"> that should be used for communicating with the local Archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ArchiveContext(AeronArchive.Context archiveContext)
            {
                this.archiveContext = archiveContext;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive.
            /// </summary>
            /// <returns> the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive. </returns>
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
                this.clusteredServiceDirectoryName = clusterDirectoryName;
                return this;
            }

            /// <summary>
            /// The directory name to use for the cluster directory.
            /// </summary>
            /// <returns> directory name for the cluster directory. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"/>
            public string ClusterDirectoryName()
            {
                return clusteredServiceDirectoryName;
            }

            /// <summary>
            /// Set the directory to use for the cluster directory.
            /// </summary>
            /// <param name="clusterDir"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME </seealso>
            public Context ClusterDir(DirectoryInfo clusterDir)
            {
                this.clusterDir = clusterDir;
                return this;
            }

            /// <summary>
            /// The directory used for for the cluster directory.
            /// </summary>
            /// <returns>  directory for for the cluster directory. </returns>
            /// <seealso cref= Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME </seealso>
            public DirectoryInfo ClusterDir()
            {
                return clusterDir;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shutdown a clustered service.
            /// </summary>
            /// <param name="barrier"> that can be used to shutdown a clustered service. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ShutdownSignalBarrier(ShutdownSignalBarrier barrier)
            {
                shutdownSignalBarrier = barrier;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shutdown a clustered service.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.Concurrent.ShutdownSignalBarrier"/> that can be used to shutdown a clustered service. </returns>
            public ShutdownSignalBarrier ShutdownSignalBarrier()
            {
                return shutdownSignalBarrier;
            }

            /// <summary>
            /// Set the <seealso cref="Action"/> that is called when processing a
            /// <seealso cref="ServiceAction.SHUTDOWN"/> or <seealso cref="ServiceAction.ABORT"/>
            /// </summary>
            /// <param name="terminationHook"> that can be used to terminate a service container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context TerminationHook(Action terminationHook)
            {
                this.terminationHook = terminationHook;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Action"/> that is called when processing a
            /// <seealso cref="ClusterAction.SHUTDOWN"/> or <seealso cref="ClusterAction.ABORT"/>
            /// <para>
            /// The default action is to call signal on the <seealso cref="#shutdownSignalBarrier()"/>.
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
            public Context MarkFile(ClusterMarkFile cncFile)
            {
                this.markFile = cncFile;
                return this;
            }

            /// <summary>
            /// The <seealso cref="Cluster.ClusterMarkFile"/> in use.
            /// </summary>
            /// <returns> CnC file in use. </returns>
            public ClusterMarkFile MarkFile()
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
                CloseHelper.QuietDispose(markFile);

                if (ownsAeronClient)
                {
                    aeron?.Dispose();
                }
            }

            private void ConcludeMarkFile()
            {
                ClusterMarkFile.CheckHeaderLength(
                    aeron.Ctx().AeronDirectoryName(),
                    archiveContext.ControlRequestChannel(),
                    ServiceControlChannel(),
                    null,
                    serviceName,
                    null);

                var encoder = markFile.Encoder();

                encoder
                    .ArchiveStreamId(archiveContext.ControlRequestStreamId())
                    .ServiceStreamId(serviceStreamId)
                    .ConsensusModuleStreamId(consensusModuleStreamId)
                    .IngressStreamId(0)
                    .MemberId(Adaptive.Aeron.Aeron.NULL_VALUE)
                    .ServiceId(serviceId)
                    .AeronDirectory(aeron.Ctx().AeronDirectoryName())
                    .ArchiveChannel(archiveContext.ControlRequestChannel())
                    .ServiceControlChannel(serviceControlChannel)
                    .IngressChannel("")
                    .ServiceName(serviceName)
                    .Authenticator("");

                markFile.UpdateActivityTimestamp(epochClock.Time());
                markFile.SignalReady();
            }
        }
    }
}
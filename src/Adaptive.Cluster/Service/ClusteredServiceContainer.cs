/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
using static Adaptive.Aeron.Aeron;
using static Adaptive.Aeron.Aeron.Context;
using static Adaptive.Aeron.ChannelUri;
using static Adaptive.Cluster.ClusterMarkFile;
using static Adaptive.Cluster.Service.ClusteredServiceContainer.Configuration;

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
        /// Default set of flags when taking a snapshot
        /// </summary>
        public const int CLUSTER_ACTION_FLAGS_DEFAULT = 0;

        /// <summary>
        /// Flag for a snapshot taken on a standby node.
        /// </summary>
        public const int CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT = 1;

        /// <summary>
        /// Launch the clustered service container and await a shutdown signal.
        /// </summary>
        /// <param name="args"> command line argument which is a list for properties files as URLs or filenames.
        /// </param>
        public static void Main(string[] args)
        {
            using (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier())
            using (
                ClusteredServiceContainer container = Launch(new Context().TerminationHook(() => barrier.SignalAll()))
            )
            {
                barrier.Await();

                Console.WriteLine("Shutdown ClusteredServiceContainer...");
            }
        }

        private readonly Context _ctx;
        private readonly AgentRunner _serviceAgentRunner;

        private ClusteredServiceContainer(Context ctx)
        {
            this._ctx = ctx;

            try
            {
                ctx.Conclude();
            }
            catch (Exception)
            {
                if (null != ctx.ClusterMarkFile())
                {
                    ctx.ClusterMarkFile().SignalFailedStart();
                    ctx.ClusterMarkFile().Force();
                }

                ctx.Dispose();
                throw;
            }

            ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
            _serviceAgentRunner = new AgentRunner(ctx.IdleStrategy(), ctx.ErrorHandler(), ctx.ErrorCounter(), agent);
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
            AgentRunner.StartOnThread(
                clusteredServiceContainer._serviceAgentRunner,
                clusteredServiceContainer._ctx.ThreadFactory()
            );

            return clusteredServiceContainer;
        }

        /// <summary>
        /// Get the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>.
        /// </summary>
        /// <returns> the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>.
        /// </returns>
        public Context Ctx()
        {
            return _ctx;
        }

        public void Dispose()
        {
            _serviceAgentRunner?.Dispose();
        }

        /// <summary>
        /// Configuration options for the consensus module and service container within a cluster.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S1118:Utility classes should not have public constructors",
            Justification = "Public ctor in shipped API surface; marking static would break consumers."
        )]
        public class Configuration
        {
            /// <summary>
            /// Type of snapshot for this service.
            /// </summary>
            public const long SNAPSHOT_TYPE_ID = 2;

            /// <summary>
            /// Update interval for cluster mark file in nanoseconds.
            /// </summary>
            public static readonly long MARK_FILE_UPDATE_INTERVAL_NS = 1_000_000_000L;

            /// <summary>
            /// Timeout in milliseconds to detect liveness.
            /// </summary>
            public static readonly long LIVENESS_TIMEOUT_MS =
                10 * TimeUnit.NANOSECONDS.ToMillis(MARK_FILE_UPDATE_INTERVAL_NS);

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
            public static readonly string REPLAY_CHANNEL_DEFAULT = IPC_CHANNEL;

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
            /// Default channel for communications between the local consensus module and services. This should be IPC.
            /// </summary>
            public const string CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=128k";

            /// <summary>
            /// Stream id within the control channel for communications from the consensus module to the services.
            /// </summary>
            public const string SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

            /// <summary>
            /// Default stream id within the control channel for communications from the consensus module to the
            /// services.
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
            /// Default stream id for the archived snapshots within a channel.
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
            /// Directory to use for the aeron cluster services, will default to <seealso cref="CLUSTER_DIR_PROP_NAME"/>
            /// if not specified.
            /// </summary>
            public const string CLUSTER_SERVICES_DIR_PROP_NAME = "aeron.cluster.services.dir";

            /// <summary>
            /// Directory to use for the Cluster component's mark file.
            /// </summary>
            public const string MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";

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
            /// Property name for threshold value for the container work cycle threshold to track for being exceeded.
            /// </summary>
            public const string CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.service.cycle.threshold";

            /// <summary>
            /// Default threshold value for the container work cycle threshold to track for being exceeded.
            /// </summary>
            public const long CYCLE_THRESHOLD_DEFAULT_NS = 1_000_000; // 1S

            /// <summary>
            /// Property name for threshold value, which is used for tracking snapshot duration breaches.
            ///
            /// <remarks>Since 1.44.0</remarks>
            /// </summary>
            public const string SNAPSHOT_DURATION_THRESHOLD_PROP_NAME = "aeron.cluster.service.snapshot.threshold";

            /// <summary>
            /// Default threshold value, which is used for tracking snapshot duration breaches.
            ///
            /// <remarks>Since 1.44.0</remarks>
            /// </summary>
            public static readonly long SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS = TimeUnit.MILLIS.ToNanos(1000);

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
            /// The value <seealso cref="CLUSTER_ID_DEFAULT"/> or system property <seealso cref="CLUSTER_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_ID_DEFAULT"/> or system property <seealso cref="CLUSTER_ID_PROP_NAME"/>
            /// if set. </returns>
            public static int ClusterId()
            {
                return Config.GetInteger(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SERVICE_ID_DEFAULT"/> or system property <seealso cref="SERVICE_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="SERVICE_ID_DEFAULT"/> or system property <seealso cref="SERVICE_ID_PROP_NAME"/>
            /// if set. </returns>
            public static int ServiceId()
            {
                return Config.GetInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SERVICE_NAME_DEFAULT"/> or system property
            /// <seealso cref="SERVICE_NAME_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SERVICE_NAME_DEFAULT"/> or system property
            /// <seealso cref="SERVICE_NAME_PROP_NAME"/> if set. </returns>
            public static string ServiceName()
            {
                return Config.GetProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="REPLAY_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="REPLAY_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="REPLAY_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="REPLAY_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ReplayChannel()
            {
                return Config.GetProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="REPLAY_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="REPLAY_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="REPLAY_STREAM_ID_PROP_NAME"/>
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
            /// The value <seealso cref="SNAPSHOT_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="SNAPSHOT_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="SNAPSHOT_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="SNAPSHOT_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string SnapshotChannel()
            {
                return Config.GetProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="SNAPSHOT_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="SNAPSHOT_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="SNAPSHOT_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="SNAPSHOT_STREAM_ID_PROP_NAME"/> if set. </returns>
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
            /// Property to configure if this node should take standby snapshots. The default for this property is
            /// <code>false</code>.
            /// </summary>
            public const string STANDBY_SNAPSHOT_ENABLED_PROP_NAME = "aeron.cluster.standby.snapshot.enabled";

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
            /// The value <seealso cref="CLUSTER_DIR_DEFAULT"/> or system property
            /// <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_DIR_DEFAULT"/> or system property
            /// <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set. </returns>
            public static string ClusterDirName()
            {
                return Config.GetProperty(CLUSTER_DIR_PROP_NAME, CLUSTER_DIR_DEFAULT);
            }

            /// <summary>
            /// The value of system property <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set or null.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_DIR_PROP_NAME"/> if set or null. </returns>
            public static string ClusterServicesDirName()
            {
                return Config.GetProperty(CLUSTER_SERVICES_DIR_PROP_NAME);
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
            /// The value <seealso cref="RESPONDER_SERVICE_DEFAULT"/> or system property
            /// <seealso cref="RESPONDER_SERVICE_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="RESPONDER_SERVICE_DEFAULT"/> or system property
            /// <seealso cref="RESPONDER_SERVICE_PROP_NAME"/> if set. </returns>
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
            /// Get threshold value for the container work cycle threshold to track for being exceeded.
            /// </summary>
            /// <returns> threshold value in nanoseconds. </returns>
            public static long CycleThresholdNs()
            {
                return Config.GetDurationInNanos(CYCLE_THRESHOLD_PROP_NAME, CYCLE_THRESHOLD_DEFAULT_NS);
            }

            /// <summary>
            /// Get threshold value, which is used for monitoring snapshot duration breaches of its predefined
            /// threshold.
            /// </summary>
            /// <returns> threshold value in nanoseconds. </returns>
            public static long SnapshotDurationThresholdNs()
            {
                return Config.GetDurationInNanos(
                    SNAPSHOT_DURATION_THRESHOLD_PROP_NAME,
                    SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS
                );
            }

            /// <summary>
            /// Get the configuration value to determine if this node should take standby snapshots be enabled.
            /// </summary>
            /// <returns> configuration value for standby snapshots being enabled. </returns>
            public static bool StandbySnapshotEnabled()
            {
                return Config.GetBoolean(STANDBY_SNAPSHOT_ENABLED_PROP_NAME);
            }

            /// <summary>
            /// Create a new <seealso cref="IClusteredService"/> based on the configured
            /// <seealso cref="SERVICE_CLASS_NAME_PROP_NAME"/>.
            /// </summary>
            /// <returns> a new <seealso cref="IClusteredService"/> based on the configured
            /// <seealso cref="SERVICE_CLASS_NAME_PROP_NAME"/>. </returns>
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
            /// Create a new <seealso cref="DelegatingErrorHandler"/> defined by
            /// <seealso cref="DELEGATING_ERROR_HANDLER_PROP_NAME"/>.
            /// </summary>
            /// <returns> a new <seealso cref="DelegatingErrorHandler"/> defined by
            /// <seealso cref="DELEGATING_ERROR_HANDLER_PROP_NAME"/> or
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

            /// <summary>
            /// Get the alternative directory to be used for storing the Cluster component's mark file.
            /// </summary>
            /// <returns> the directory to be used for storing the archive mark file. </returns>
            public static string MarkFileDir()
            {
                return Config.GetProperty(MARK_FILE_DIR_PROP_NAME);
            }
        }

        /// <summary>
        /// The context will be owned by <seealso cref="ClusteredServiceAgent"/> after a successful
        /// <seealso cref="ClusteredServiceContainer.Launch(Context)"/> and closed via
        /// <seealso cref="ClusteredServiceContainer.Dispose()"/>.
        /// </summary>
        public class Context
        {
            private int _isConcluded = 0;
            private int _appVersion = SemanticVersion.Compose(0, 0, 1);
            private int _clusterId = Configuration.ClusterId();
            private int _serviceId = Configuration.ServiceId();
            private string _serviceName = Config.GetProperty(SERVICE_NAME_PROP_NAME);
            private string _replayChannel = Configuration.ReplayChannel();
            private int _replayStreamId = Configuration.ReplayStreamId();
            private string _controlChannel = Configuration.ControlChannel();
            private int _consensusModuleStreamId = Configuration.ConsensusModuleStreamId();
            private int _serviceStreamId = Configuration.ServiceStreamId();
            private string _snapshotChannel = Configuration.SnapshotChannel();
            private int _snapshotStreamId = Configuration.SnapshotStreamId();
            private int _errorBufferLength = Configuration.ErrorBufferLength();
            private bool _isRespondingService = Configuration.IsRespondingService();
            private int _logFragmentLimit = Configuration.LogFragmentLimit();
            private long _cycleThresholdNs = Configuration.CycleThresholdNs();
            private long _snapshotDurationThresholdNs = Configuration.SnapshotDurationThresholdNs();
            private bool _standbySnapshotEnabled = Configuration.StandbySnapshotEnabled();

            private CountdownEvent _abortLatch;
            private IThreadFactory _threadFactory;
            private Func<IIdleStrategy> _idleStrategySupplier;
            private IEpochClock _epochClock;
            private INanoClock _nanoClock;
            private DistinctErrorLog _errorLog;
            private IErrorHandler _errorHandler;
            private DelegatingErrorHandler _delegatingErrorHandler;
            private AtomicCounter _errorCounter;
            private CountedErrorHandler _countedErrorHandler;
            private AeronArchive.Context _archiveContext;
            private string _clusterDirectoryName = ClusterDirName();
            private DirectoryInfo _clusterDir;
            private DirectoryInfo _markFileDir;
            private string _aeronDirectoryName = GetAeronDirectoryName();
            private Aeron.Aeron _aeron;
            private DutyCycleTracker _dutyCycleTracker;
            private SnapshotDurationTracker _snapshotDurationTracker;
            private AppVersionValidator _appVersionValidator;
            private bool _ownsAeronClient;

            private IClusteredService _clusteredService;
            private Action _terminationHook;
            private ClusterMarkFile _markFile;

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
            // Upstream: io.aeron.cluster.service.ClusteredServiceContainer.Context#conclude
            // is @SuppressWarnings("MethodLength").
            [System.Diagnostics.CodeAnalysis.SuppressMessage(
                "Major Code Smell",
                "S138:Functions should not have too many lines",
                Justification = "Upstream Java parity; method is itself @SuppressWarnings(\"MethodLength\")."
            )]
            [System.Diagnostics.CodeAnalysis.SuppressMessage(
                "Maintainability",
                "CA1502:Avoid excessive complexity",
                Justification = "Branch shape mirrors upstream Java (see Upstream comment above)."
            )]
            [System.Diagnostics.CodeAnalysis.SuppressMessage(
                "Maintainability",
                "CA1506:Avoid excessive class coupling",
                Justification = "Coupling mirrors upstream Java context-conclude (see Upstream comment above)."
            )]
            public void Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

                if (_serviceId < 0 || _serviceId > 127)
                {
                    throw new ConfigurationException("service id outside allowed range (0-127): " + _serviceId);
                }

                if (null == _threadFactory)
                {
                    _threadFactory = new DefaultThreadFactory();
                }

                if (null == _idleStrategySupplier)
                {
                    _idleStrategySupplier = Configuration.IdleStrategySupplier(null);
                }

                if (null == _appVersionValidator)
                {
                    _appVersionValidator = Cluster.AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
                }

                if (null == _epochClock)
                {
                    _epochClock = SystemEpochClock.INSTANCE;
                }

                if (null == _nanoClock)
                {
                    _nanoClock = SystemNanoClock.INSTANCE;
                }

                if (null == _clusterDir)
                {
                    _clusterDir = new DirectoryInfo(_clusterDirectoryName);
                }

                if (null == _markFileDir)
                {
                    String dir = Configuration.MarkFileDir();
                    _markFileDir = string.IsNullOrEmpty(dir) ? _clusterDir : new DirectoryInfo(dir);
                }

                _clusterDir = new DirectoryInfo(Path.GetFullPath(_clusterDir.FullName));
                _clusterDirectoryName = Path.GetFullPath(_clusterDir.FullName);
                _markFileDir = new DirectoryInfo(Path.GetFullPath(_markFileDir.FullName));

                IoUtil.EnsureDirectoryExists(_clusterDir, "cluster");
                IoUtil.EnsureDirectoryExists(_markFileDir, "mark file");

                if (null == _markFile)
                {
                    int filePageSize =
                        null != _aeron
                            ? _aeron.Ctx.FilePageSize()
                            : DriverFilePageSize(
                                new DirectoryInfo(_aeronDirectoryName),
                                _epochClock,
                                new Aeron.Aeron.Context().DriverTimeoutMs()
                            );
                    _markFile = new ClusterMarkFile(
                        new FileInfo(Path.Combine(_markFileDir.FullName, MarkFilenameForService(_serviceId))),
                        ClusterComponentType.CONTAINER,
                        _errorBufferLength,
                        _epochClock,
                        LIVENESS_TIMEOUT_MS,
                        filePageSize
                    );
                }

                MarkFile.EnsureMarkFileLink(
                    _clusterDir,
                    new FileInfo(Path.Combine(_markFileDir.FullName, MarkFilenameForService(_serviceId))),
                    LinkFilenameForService(_serviceId)
                );

                if (null == _errorLog)
                {
                    _errorLog = new DistinctErrorLog(_markFile.ErrorBuffer, _epochClock); // US_ASCII
                }

                _errorHandler = SetupErrorHandler(_errorHandler, _errorLog);

                if (null == _delegatingErrorHandler)
                {
                    _delegatingErrorHandler = NewDelegatingErrorHandler();
                    if (null != _delegatingErrorHandler)
                    {
                        _delegatingErrorHandler.Next(_errorHandler);
                        _errorHandler = _delegatingErrorHandler;
                    }
                }
                else
                {
                    _delegatingErrorHandler.Next(_errorHandler);
                    _errorHandler = _delegatingErrorHandler;
                }

                if (string.IsNullOrEmpty(_serviceName))
                {
                    _serviceName = "clustered-service-" + _clusterId + "-" + _serviceId;
                }

                if (null == _aeron)
                {
                    _aeron = Connect(
                        new Aeron.Aeron.Context()
                            .AeronDirectoryName(_aeronDirectoryName)
                            .ErrorHandler(_errorHandler)
                            .SubscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                            .AwaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                            .EpochClock(_epochClock)
                            .ClientName(_serviceName)
                    );

                    _ownsAeronClient = true;
                }

                if (_aeron.Ctx.SubscriberErrorHandler() != RethrowingErrorHandler.INSTANCE)
                {
                    throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
                }

                ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();
                if (null == _errorCounter)
                {
                    _errorCounter = ClusterCounters.AllocateServiceErrorCounter(
                        _aeron,
                        tempBuffer,
                        _clusterId,
                        _serviceId
                    );
                }

                if (null == _countedErrorHandler)
                {
                    _countedErrorHandler = new CountedErrorHandler(_errorHandler, _errorCounter);
                    if (_ownsAeronClient)
                    {
                        _aeron.Ctx.ErrorHandler(_countedErrorHandler);
                    }
                }

                if (null == _dutyCycleTracker)
                {
                    _dutyCycleTracker = new DutyCycleStallTracker(
                        ClusterCounters.AllocateServiceCounter(
                            _aeron,
                            tempBuffer,
                            "Cluster container max cycle time in ns",
                            AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
                            _clusterId,
                            _serviceId
                        ),
                        ClusterCounters.AllocateServiceCounter(
                            _aeron,
                            tempBuffer,
                            "Cluster container work cycle time exceeded count: threshold=" + _cycleThresholdNs + "ns",
                            AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                            _clusterId,
                            _serviceId
                        ),
                        _cycleThresholdNs
                    );
                }

                if (null == _snapshotDurationTracker)
                {
                    _snapshotDurationTracker = new SnapshotDurationTracker(
                        ClusterCounters.AllocateServiceCounter(
                            _aeron,
                            tempBuffer,
                            "Clustered service max snapshot duration in ns",
                            AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
                            _clusterId,
                            _serviceId
                        ),
                        ClusterCounters.AllocateServiceCounter(
                            _aeron,
                            tempBuffer,
                            "Clustered service max snapshot duration exceeded count: threshold="
                                + _snapshotDurationThresholdNs,
                            AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
                            _clusterId,
                            _serviceId
                        ),
                        _snapshotDurationThresholdNs
                    );
                }

                if (null == _archiveContext)
                {
                    _archiveContext = new AeronArchive.Context()
                        .ControlRequestChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlResponseChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlRequestStreamId(AeronArchive.Configuration.LocalControlStreamId())
                        .ControlResponseStreamId(
                            _clusterId * 100
                                + 100
                                + AeronArchive.Configuration.ControlResponseStreamId()
                                + (_serviceId + 1)
                        );
                }

                if (!_archiveContext.ControlRequestChannel().StartsWith(IPC_CHANNEL))
                {
                    throw new ClusterException("local archive control must be IPC");
                }

                if (!_archiveContext.ControlResponseChannel().StartsWith(IPC_CHANNEL))
                {
                    throw new ClusterException("local archive control must be IPC");
                }

                _archiveContext
                    .AeronClient(_aeron)
                    .OwnsAeronClient(false)
                    .Lock(NoOpLock.Instance)
                    .ErrorHandler(_countedErrorHandler)
                    .ControlRequestChannel(
                        AddAliasIfAbsent(
                            _archiveContext.ControlRequestChannel(),
                            "sc-" + _serviceId + "-archive-ctrl-req-cluster-" + _clusterId
                        )
                    )
                    .ControlResponseChannel(
                        AddAliasIfAbsent(
                            _archiveContext.ControlResponseChannel(),
                            "sc-" + _serviceId + "-archive-ctrl-resp-cluster-" + _clusterId
                        )
                    )
                    .ClientName(_serviceName);

                if (null == _terminationHook)
                {
                    _terminationHook = () => { };
                }

                if (null == _clusteredService)
                {
                    _clusteredService = NewClusteredService();
                }

                _abortLatch = new CountdownEvent(_aeron.Ctx.UseConductorAgentInvoker() ? 1 : 0);
                ConcludeMarkFile();

                if (ShouldPrintConfigurationOnStart())
                {
                    Console.WriteLine(this);
                }
            }

            /// <summary>
            /// Has the context had the <seealso cref="Conclude()"/> method called.
            /// </summary>
            /// <returns> true of the <seealso cref="Conclude()"/> method has been called. </returns>
            public bool Concluded => Volatile.Read(ref _isConcluded) == 1;

            /// <summary>
            /// User assigned application version which appended to the log as the appVersion in new leadership events.
            /// <para>
            /// This can be validated using <seealso cref="SemanticVersion"/> to ensure only application nodes of the
            /// same major version communicate with each other.
            ///
            /// </para>
            /// </summary>
            /// <param name="appVersion"> for user application. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AppVersion(int appVersion)
            {
                this._appVersion = appVersion;
                return this;
            }

            /// <summary>
            /// User assigned application version which appended to the log as the appVersion in new leadership events.
            /// <para>
            /// This can be validated using <seealso cref="SemanticVersion"/> to ensure only application nodes of the
            /// same major version communicate with each other.
            ///
            /// </para>
            /// </summary>
            /// <returns> appVersion for user application. </returns>
            public int AppVersion()
            {
                return _appVersion;
            }

            /// <summary>
            /// User assigned application version validator implementation used to check version compatibility.
            /// <para>
            /// The default validator uses <seealso cref="SemanticVersion"/> semantics.
            ///
            /// </para>
            /// </summary>
            /// <param name="appVersionValidator"> for user application. </param>
            /// <returns> this for fluent API. </returns>
            public Context AppVersionValidator(AppVersionValidator appVersionValidator)
            {
                this._appVersionValidator = appVersionValidator;
                return this;
            }

            /// <summary>
            /// User assigned application version validator implementation used to check version compatibility.
            /// <para>
            /// The default is to use <seealso cref="SemanticVersion"/> major version for checking compatibility.
            ///
            /// </para>
            /// </summary>
            /// <returns> AppVersionValidator in use. </returns>
            public AppVersionValidator AppVersionValidator()
            {
                return _appVersionValidator;
            }

            /// <summary>
            /// Set the id for this cluster instance. This must match with the Consensus Module.
            /// </summary>
            /// <param name="clusterId"> for this clustered instance. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CLUSTER_ID_PROP_NAME"></seealso>
            public Context ClusterId(int clusterId)
            {
                this._clusterId = clusterId;
                return this;
            }

            /// <summary>
            /// Get the id for this cluster instance. This must match with the Consensus Module.
            /// </summary>
            /// <returns> the id for this cluster instance. </returns>
            /// <seealso cref="Configuration.CLUSTER_ID_PROP_NAME"></seealso>
            public int ClusterId()
            {
                return _clusterId;
            }

            /// <summary>
            /// Set the id for this clustered service. Services should be numbered from 0 and be contiguous.
            /// </summary>
            /// <param name="serviceId"> for this clustered service. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SERVICE_ID_PROP_NAME"></seealso>
            public Context ServiceId(int serviceId)
            {
                this._serviceId = serviceId;
                return this;
            }

            /// <summary>
            /// Get the id for this clustered service. Services should be numbered from 0 and be contiguous.
            /// </summary>
            /// <returns> the id for this clustered service. </returns>
            /// <seealso cref="Configuration.SERVICE_ID_PROP_NAME"></seealso>
            public int ServiceId()
            {
                return _serviceId;
            }

            /// <summary>
            /// Set the name for a clustered service to be the <see cref="IAgent.RoleName"/> for the
            /// <seealso cref="IAgent"/>.
            /// </summary>
            /// <param name="serviceName"> for a clustered service to be the role for the <seealso cref="IAgent"/>.
            /// </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.SERVICE_NAME_PROP_NAME"></seealso>
            public Context ServiceName(string serviceName)
            {
                this._serviceName = serviceName;
                return this;
            }

            /// <summary>
            /// Get the name for a clustered service to be the <see cref="IAgent.RoleName"/> for the
            /// <seealso cref="IAgent"/>.
            /// </summary>
            /// <returns> the name for a clustered service to be the role of the <seealso cref="IAgent"/>. </returns>
            /// <seealso cref="Configuration.SERVICE_NAME_PROP_NAME"></seealso>
            public string ServiceName()
            {
                return _serviceName;
            }

            /// <summary>
            /// Set the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="channel"> parameter for the cluster log replay channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.REPLAY_CHANNEL_PROP_NAME"></seealso>
            public Context ReplayChannel(string channel)
            {
                _replayChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the channel parameter for the cluster replay channel. </returns>
            /// <seealso cref="Configuration.REPLAY_CHANNEL_PROP_NAME"></seealso>
            public string ReplayChannel()
            {
                return _replayChannel;
            }

            /// <summary>
            /// Set the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="streamId"> for the cluster log replay channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.REPLAY_STREAM_ID_PROP_NAME"></seealso>
            public Context ReplayStreamId(int streamId)
            {
                _replayStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the stream id for the cluster log replay channel. </returns>
            /// <seealso cref="Configuration.REPLAY_STREAM_ID_PROP_NAME"></seealso>
            public int ReplayStreamId()
            {
                return _replayStreamId;
            }

            /// <summary>
            /// Set the channel parameter for bidirectional communications between the consensus module and services.
            /// </summary>
            /// <param name="channel"> parameter for sending messages to the Consensus Module. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public Context ControlChannel(string channel)
            {
                _controlChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for bidirectional communications between the consensus module and services.
            /// </summary>
            /// <returns> the channel parameter for sending messages to the Consensus Module. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public string ControlChannel()
            {
                return _controlChannel;
            }

            /// <summary>
            /// Set the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <param name="streamId"> for communications from the consensus module and to the services. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SERVICE_STREAM_ID_PROP_NAME"></seealso>
            public Context ServiceStreamId(int streamId)
            {
                _serviceStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the consensus module and to the services.
            /// </summary>
            /// <returns> the stream id for communications from the consensus module and to the services. </returns>
            /// <seealso cref="Configuration.SERVICE_STREAM_ID_PROP_NAME"></seealso>
            public int ServiceStreamId()
            {
                return _serviceStreamId;
            }

            /// <summary>
            /// Set the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <param name="streamId"> for communications from the services to the consensus module. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CONSENSUS_MODULE_STREAM_ID_PROP_NAME"></seealso>
            public Context ConsensusModuleStreamId(int streamId)
            {
                _consensusModuleStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for communications from the services to the consensus module.
            /// </summary>
            /// <returns> the stream id for communications from the services to the consensus module. </returns>
            /// <seealso cref="Configuration.CONSENSUS_MODULE_STREAM_ID_PROP_NAME"></seealso>
            public int ConsensusModuleStreamId()
            {
                return _consensusModuleStreamId;
            }

            /// <summary>
            /// Set the channel parameter for snapshot recordings.
            /// </summary>
            /// <param name="channel"> parameter for snapshot recordings </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_CHANNEL_PROP_NAME"></seealso>
            public Context SnapshotChannel(string channel)
            {
                _snapshotChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for snapshot recordings.
            /// </summary>
            /// <returns> the channel parameter for snapshot recordings. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_CHANNEL_PROP_NAME"></seealso>
            public string SnapshotChannel()
            {
                return _snapshotChannel;
            }

            /// <summary>
            /// Set the stream id for snapshot recordings.
            /// </summary>
            /// <param name="streamId"> for snapshot recordings. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.SNAPSHOT_STREAM_ID_PROP_NAME"></seealso>
            public Context SnapshotStreamId(int streamId)
            {
                _snapshotStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for snapshot recordings.
            /// </summary>
            /// <returns> the stream id for snapshot recordings. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_STREAM_ID_PROP_NAME"></seealso>
            public int SnapshotStreamId()
            {
                return _snapshotStreamId;
            }

            /// <summary>
            /// Set if this a service that responds to client requests.
            /// </summary>
            /// <param name="isRespondingService"> true if this service responds to client requests, otherwise false.
            /// </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.RESPONDER_SERVICE_PROP_NAME"></seealso>
            public Context IsRespondingService(bool isRespondingService)
            {
                this._isRespondingService = isRespondingService;
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
                this._logFragmentLimit = logFragmentLimit;
                return this;
            }

            /// <summary>
            /// Get the fragment limit to be used when polling the log <seealso cref="Subscription"/>.
            /// </summary>
            /// <returns> the fragment limit to be used when polling the log <seealso cref="Subscription"/>. </returns>
            /// <seealso cref="Configuration.LOG_FRAGMENT_LIMIT_DEFAULT"/>
            public int LogFragmentLimit()
            {
                return _logFragmentLimit;
            }

            /// <summary>
            /// Is this a service that responds to client requests?
            /// </summary>
            /// <returns> true if this service responds to client requests, otherwise false. </returns>
            /// <seealso cref="Configuration.RESPONDER_SERVICE_PROP_NAME"></seealso>
            public bool IsRespondingService()
            {
                return _isRespondingService;
            }

            /// <summary>
            /// Get the thread factory used for creating threads.
            /// </summary>
            /// <returns> thread factory used for creating threads. </returns>
            public IThreadFactory ThreadFactory()
            {
                return _threadFactory;
            }

            /// <summary>
            /// Set the thread factory used for creating threads.
            /// </summary>
            /// <param name="threadFactory"> used for creating threads </param>
            /// <returns> this for a fluent API. </returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                this._threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// Provides an <seealso cref="IIdleStrategy"/> supplier for the idle strategy for the agent duty cycle.
            /// </summary>
            /// <param name="idleStrategySupplier"> supplier for the idle strategy for the agent duty cycle. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategySupplier(Func<IIdleStrategy> idleStrategySupplier)
            {
                this._idleStrategySupplier = idleStrategySupplier;
                return this;
            }

            /// <summary>
            /// Get a new <seealso cref="IdleStrategy"/> based on configured supplier.
            /// </summary>
            /// <returns> a new <seealso cref="IdleStrategy"/> based on configured supplier. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return _idleStrategySupplier();
            }

            /// <summary>
            /// Set the <seealso cref="IEpochClock"/> to be used for tracking wall clock time when interacting with the
            /// container.
            /// </summary>
            /// <param name="clock"> <seealso cref="IEpochClock"/> to be used for tracking wall clock time when
            /// interacting with the container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EpochClock(IEpochClock clock)
            {
                this._epochClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IEpochClock"/> to used for tracking wall clock time within the container.
            /// </summary>
            /// <returns> the <seealso cref="IEpochClock"/> to used for tracking wall clock time within the container.
            /// </returns>
            public IEpochClock EpochClock()
            {
                return _epochClock;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.ErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.ErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>. </returns>
            public IErrorHandler ErrorHandler()
            {
                return _errorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.ErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>.
            /// </summary>
            /// <param name="errorHandler"> the error handler to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>. </param>
            /// <returns> this for a fluent API </returns>
            public Context ErrorHandler(IErrorHandler errorHandler)
            {
                this._errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/> which will
            /// delegate to <seealso cref="ErrorHandler()"/> as next in the chain.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>. </returns>
            /// <seealso cref="Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME"></seealso>
            public DelegatingErrorHandler DelegatingErrorHandler()
            {
                return _delegatingErrorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.DelegatingErrorHandler"/> to be used by the
            /// <seealso cref="ClusteredServiceContainer"/> which will
            /// delegate to <seealso cref="ErrorHandler()"/> as next in the chain.
            /// </summary>
            /// <param name="delegatingErrorHandler"> the error handler to be used by the
            /// <seealso cref="ClusteredServiceContainer"/>. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME"> </seealso>
            public Context DelegatingErrorHandler(DelegatingErrorHandler delegatingErrorHandler)
            {
                this._delegatingErrorHandler = delegatingErrorHandler;
                return this;
            }

            /// <summary>
            /// Get the error counter that will record the number of errors the container has observed.
            /// </summary>
            /// <returns> the error counter that will record the number of errors the container has observed. </returns>
            public AtomicCounter ErrorCounter()
            {
                return _errorCounter;
            }

            /// <summary>
            /// Set the error counter that will record the number of errors the cluster node has observed.
            /// </summary>
            /// <param name="errorCounter"> the error counter that will record the number of errors the cluster node has
            /// observed. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorCounter(AtomicCounter errorCounter)
            {
                this._errorCounter = errorCounter;
                return this;
            }

            /// <summary>
            /// Non-default for context.
            /// </summary>
            /// <param name="countedErrorHandler"> to override the default. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CountedErrorHandler(CountedErrorHandler countedErrorHandler)
            {
                this._countedErrorHandler = countedErrorHandler;
                return this;
            }

            /// <summary>
            /// The <seealso cref="ErrorHandler()"/> that will increment <seealso cref="ErrorCounter()"/> by default.
            /// </summary>
            /// <returns> <seealso cref="ErrorHandler()"/> that will increment <seealso cref="ErrorCounter()"/> by
            /// default. </returns>
            public CountedErrorHandler CountedErrorHandler()
            {
                return _countedErrorHandler;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <param name="aeronDirectoryName"> the top level Aeron directory. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                this._aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return _aeronDirectoryName;
            }

            /// <summary>
            /// An <seealso cref="Adaptive.Aeron.Aeron"/> client for the container.
            /// </summary>
            /// <returns> <seealso cref="Adaptive.Aeron.Aeron"/> client for the container </returns>
            public Aeron.Aeron AeronClient()
            {
                return _aeron;
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
            public Context AeronClient(Aeron.Aeron aeron)
            {
                this._aeron = aeron;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility for
            /// closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="AeronClient()"/> client.
            /// </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this._ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility for
            /// closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility
            /// for closing it? </returns>
            public bool OwnsAeronClient()
            {
                return _ownsAeronClient;
            }

            /// <summary>
            /// The service this container holds.
            /// </summary>
            /// <returns> service this container holds. </returns>
            public IClusteredService ClusteredService()
            {
                return _clusteredService;
            }

            /// <summary>
            /// Set the service this container is to hold.
            /// </summary>
            /// <param name="clusteredService"> this container is to hold. </param>
            /// <returns> this for fluent API. </returns>
            public Context ClusteredService(IClusteredService clusteredService)
            {
                this._clusteredService = clusteredService;
                return this;
            }

            /// <summary>
            /// Set the context that should be used for communicating with the local Archive.
            /// </summary>
            /// <param name="archiveContext"> that should be used for communicating with the local Archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ArchiveContext(AeronArchive.Context archiveContext)
            {
                this._archiveContext = archiveContext;
                return this;
            }

            /// <summary>
            /// Get the context that should be used for communicating with the local Archive.
            /// </summary>
            /// <returns> the context that should be used for communicating with the local Archive. </returns>
            public AeronArchive.Context ArchiveContext()
            {
                return _archiveContext;
            }

            /// <summary>
            /// Set the directory name to use for the consensus module directory..
            /// </summary>
            /// <param name="clusterDirectoryName"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"/>
            public Context ClusterDirectoryName(string clusterDirectoryName)
            {
                this._clusterDirectoryName = clusterDirectoryName;
                return this;
            }

            /// <summary>
            /// The directory name to use for the cluster directory.
            /// </summary>
            /// <returns> directory name for the cluster directory. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"/>
            public string ClusterDirectoryName()
            {
                return _clusterDirectoryName;
            }

            /// <summary>
            /// Set the directory to use for the cluster directory.
            /// </summary>
            /// <param name="clusterDir"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"></seealso>
            public Context ClusterDir(DirectoryInfo clusterDir)
            {
                this._clusterDir = clusterDir;
                return this;
            }

            /// <summary>
            /// The directory used for the cluster directory.
            /// </summary>
            /// <returns>  directory for the cluster directory. </returns>
            /// <seealso cref="Configuration.CLUSTER_DIR_PROP_NAME"></seealso>
            public DirectoryInfo ClusterDir()
            {
                return _clusterDir;
            }

            /// <summary>
            /// Get the directory in which the ClusteredServiceContainer will store mark file (i.e.
            /// {@code cluster-mark-service-0.dat}). It defaults to <seealso cref="ClusterDir()"/> if it is not set
            /// explicitly via the {@link ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME}.
            /// </summary>
            /// <returns> the directory in which the ClusteredServiceContainer will store mark file (i.e.
            ///         <code>cluster-mark-service-0.dat</code>). </returns>
            /// <seealso cref="ClusteredServiceContainer.Configuration.MARK_FILE_DIR_PROP_NAME"/>
            /// <seealso cref="ClusterDir()"/>
            public DirectoryInfo MarkFileDir()
            {
                return _markFileDir;
            }

            /// <summary>
            /// Set the directory in which the ClusteredServiceContainer will store mark file (i.e.
            /// {@code cluster-mark-service-0.dat}).
            /// </summary>
            /// <param name="markFileDir"> the directory in which the ClusteredServiceContainer will store mark file
            /// (i.e. {@code cluster-mark-service-0.dat}). </param>
            /// <returns> this for a fluent API. </returns>
            public ClusteredServiceContainer.Context MarkFileDir(DirectoryInfo markFileDir)
            {
                this._markFileDir = markFileDir;
                return this;
            }

            /// <summary>
            /// Set the <seealso cref="Action"/> that is called when container is instructed to terminate.
            /// </summary>
            /// <param name="terminationHook"> that can be used to terminate a service container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context TerminationHook(Action terminationHook)
            {
                this._terminationHook = terminationHook;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Action"/> that is called when container is instructed to terminate.
            /// </summary>
            /// <returns> the <seealso cref="Action"/> that can be used to terminate a service container. </returns>
            public Action TerminationHook()
            {
                return _terminationHook;
            }

            /// <summary>
            /// Set the <seealso cref="Cluster.ClusterMarkFile"/> in use.
            /// </summary>
            /// <param name="cncFile"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ClusterMarkFile(ClusterMarkFile cncFile)
            {
                this._markFile = cncFile;
                return this;
            }

            /// <summary>
            /// The <seealso cref="Cluster.ClusterMarkFile"/> in use.
            /// </summary>
            /// <returns> CnC file in use. </returns>
            public ClusterMarkFile ClusterMarkFile()
            {
                return _markFile;
            }

            /// <summary>
            /// Set the error buffer length in bytes to use.
            /// </summary>
            /// <param name="errorBufferLength"> in bytes to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorBufferLength(int errorBufferLength)
            {
                this._errorBufferLength = errorBufferLength;
                return this;
            }

            /// <summary>
            /// The error buffer length in bytes.
            /// </summary>
            /// <returns> error buffer length in bytes. </returns>
            public int ErrorBufferLength()
            {
                return _errorBufferLength;
            }

            /// <summary>
            /// Set the <seealso cref="DistinctErrorLog"/> in use.
            /// </summary>
            /// <param name="errorLog"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorLog(DistinctErrorLog errorLog)
            {
                this._errorLog = errorLog;
                return this;
            }

            /// <summary>
            /// The <seealso cref="DistinctErrorLog"/> in use.
            /// </summary>
            /// <returns> <seealso cref="DistinctErrorLog"/> in use. </returns>
            public DistinctErrorLog ErrorLog()
            {
                return _errorLog;
            }

            /// <summary>
            /// The <seealso cref="INanoClock"/> as a source of time in nanoseconds for measuring duration.
            /// </summary>
            /// <returns> the <seealso cref="INanoClock"/> as a source of time in nanoseconds for measuring duration.
            /// </returns>
            public INanoClock NanoClock()
            {
                return _nanoClock;
            }

            /// <summary>
            /// The <seealso cref="INanoClock"/> as a source of time in nanoseconds for measuring duration.
            /// </summary>
            /// <param name="clock"> to be used. </param>
            /// <returns> this for a fluent API. </returns>
            public Context NanoClock(INanoClock clock)
            {
                _nanoClock = clock;
                return this;
            }

            /// <summary>
            /// Set a threshold for the container work cycle time which when exceed it will increment the counter.
            /// </summary>
            /// <param name="thresholdNs"> value in nanoseconds </param>
            /// <returns> this for fluent API. </returns>
            /// <seealso cref="Configuration.CYCLE_THRESHOLD_PROP_NAME"/>
            /// <seealso cref="Configuration.CYCLE_THRESHOLD_DEFAULT_NS"/>
            public Context CycleThresholdNs(long thresholdNs)
            {
                this._cycleThresholdNs = thresholdNs;
                return this;
            }

            /// <summary>
            /// Threshold for the container work cycle time which when exceed it will increment the counter.
            /// </summary>
            /// <returns> threshold to track for the container work cycle time. </returns>
            public long CycleThresholdNs()
            {
                return _cycleThresholdNs;
            }

            /// <summary>
            /// Set a duty cycle tracker to be used for tracking the duty cycle time of the container.
            /// </summary>
            /// <param name="dutyCycleTracker"> to use for tracking. </param>
            /// <returns> this for fluent API. </returns>
            public Context DutyCycleTracker(DutyCycleTracker dutyCycleTracker)
            {
                this._dutyCycleTracker = dutyCycleTracker;
                return this;
            }

            /// <summary>
            /// The duty cycle tracker used to track the container duty cycle.
            /// </summary>
            /// <returns> the duty cycle tracker. </returns>
            public DutyCycleTracker DutyCycleTracker()
            {
                return _dutyCycleTracker;
            }

            /// <summary>
            /// Set a threshold for snapshot duration which when exceeded will result in a counter increment.
            /// </summary>
            /// <param name="thresholdNs"> value in nanoseconds. </param>
            /// <returns> this for fluent API. </returns>
            /// <seealso cref="Configuration.SNAPSHOT_DURATION_THRESHOLD_PROP_NAME"/>
            /// <seealso cref="Configuration.SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS"/>
            /// <remarks>since 1.44.0</remarks>
            public Context SnapshotDurationThresholdNs(long thresholdNs)
            {
                this._snapshotDurationThresholdNs = thresholdNs;
                return this;
            }

            /// <summary>
            /// Threshold for snapshot duration which when exceeded will result in a counter increment.
            /// </summary>
            /// <returns> threshold value in nanoseconds.</returns>
            /// <remarks>since 1.44.0</remarks>
            public long SnapshotDurationThresholdNs()
            {
                return _snapshotDurationThresholdNs;
            }

            /// <summary>
            /// Set snapshot duration tracker used for monitoring snapshot duration.
            /// </summary>
            /// <param name="snapshotDurationTracker"> snapshot duration tracker. </param>
            /// <returns> this for fluent API.</returns>
            /// <remarks>since 1.44.0</remarks>
            public Context SnapshotDurationTracker(SnapshotDurationTracker snapshotDurationTracker)
            {
                this._snapshotDurationTracker = snapshotDurationTracker;
                return this;
            }

            /// <summary>
            /// Get snapshot duration tracker used for monitoring snapshot duration.
            /// </summary>
            /// <returns> snapshot duration tracker.
            /// @since 1.44.0 </returns>
            public SnapshotDurationTracker SnapshotDurationTracker()
            {
                return _snapshotDurationTracker;
            }

            /// <summary>
            /// Delete the cluster container directory.
            /// </summary>
            public void DeleteDirectory()
            {
                if (null != _clusterDir)
                {
                    IoUtil.Delete(_clusterDir, false);
                }
            }

            /// <summary>
            /// Indicates if this node should take standby snapshots.
            /// </summary>
            /// <returns> <code>true</code> if this should take standby snapshots, <code>false</code> otherwise.
            /// </returns>
            /// <seealso cref="ClusteredServiceContainer.Configuration.STANDBY_SNAPSHOT_ENABLED_PROP_NAME"/>
            /// <seealso cref="ClusteredServiceContainer.Configuration.StandbySnapshotEnabled()"/>
            public bool StandbySnapshotEnabled()
            {
                return _standbySnapshotEnabled;
            }

            /// <summary>
            /// Indicates if this node should take standby snapshots.
            /// </summary>
            /// <param name="standbySnapshotEnabled"> if this node should take standby snapshots. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="ClusteredServiceContainer.Configuration.STANDBY_SNAPSHOT_ENABLED_PROP_NAME"/>
            /// <seealso cref="ClusteredServiceContainer.Configuration.StandbySnapshotEnabled()"/>
            public ClusteredServiceContainer.Context StandbySnapshotEnabled(bool standbySnapshotEnabled)
            {
                this._standbySnapshotEnabled = standbySnapshotEnabled;
                return this;
            }

            /// <summary>
            /// Close the context and free applicable resources.
            /// <para>
            /// If <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="AeronClient()"/> client will be
            /// closed.
            /// </para>
            /// </summary>
            public void Dispose()
            {
                var errorHandler = CountedErrorHandler();
                if (_ownsAeronClient)
                {
                    CloseHelper.Dispose(errorHandler, _aeron);
                }

                CloseHelper.Dispose(_markFile);
            }

            internal CountdownEvent AbortLatch()
            {
                return _abortLatch;
            }

            private void ConcludeMarkFile()
            {
                CheckHeaderLength(_aeron.Ctx.AeronDirectoryName(), ControlChannel(), null, _serviceName, null);

                var encoder = _markFile.Encoder();

                encoder
                    .ArchiveStreamId(_archiveContext.ControlRequestStreamId())
                    .ServiceStreamId(_serviceStreamId)
                    .ConsensusModuleStreamId(_consensusModuleStreamId)
                    .IngressStreamId(NULL_VALUE)
                    .MemberId(NULL_VALUE)
                    .ServiceId(_serviceId)
                    .ClusterId(_clusterId)
                    .AeronDirectory(_aeron.Ctx.AeronDirectoryName())
                    .ControlChannel(_controlChannel)
                    .IngressChannel(string.Empty)
                    .ServiceName(_serviceName)
                    .Authenticator(string.Empty);

                _markFile.UpdateActivityTimestamp(_epochClock.Time());
                _markFile.SignalReady();
                _markFile.Force();
            }

            /// <summary>
            /// {@inheritDoc}
            /// </summary>
            public override string ToString()
            {
                return
                    "ClusteredServiceContainer.Context" +
                    "\n{" +
                    "\n    isConcluded=" + Concluded +
                    "\n    ownsAeronClient=" + _ownsAeronClient +
                    "\n    aeronDirectoryName='" + _aeronDirectoryName + '\'' +
                    "\n    aeron=" + _aeron +
                    "\n    archiveContext=" + _archiveContext +
                    "\n    clusterDirectoryName='" + _clusterDirectoryName + '\'' +
                    "\n    clusterDir=" + _clusterDir +
                    "\n    appVersion=" + _appVersion +
                    "\n    clusterId=" + _clusterId +
                    "\n    serviceId=" + _serviceId +
                    "\n    serviceName='" + _serviceName + '\'' +
                    "\n    replayChannel='" + _replayChannel + '\'' +
                    "\n    replayStreamId=" + _replayStreamId +
                    "\n    controlChannel='" + _controlChannel + '\'' +
                    "\n    consensusModuleStreamId=" + _consensusModuleStreamId +
                    "\n    serviceStreamId=" + _serviceStreamId +
                    "\n    snapshotChannel='" + _snapshotChannel + '\'' +
                    "\n    snapshotStreamId=" + _snapshotStreamId +
                    "\n    errorBufferLength=" + _errorBufferLength +
                    "\n    isRespondingService=" + _isRespondingService +
                    "\n    logFragmentLimit=" + _logFragmentLimit +
                    "\n    abortLatch=" + _abortLatch +
                    "\n    threadFactory=" + _threadFactory +
                    "\n    idleStrategySupplier=" + _idleStrategySupplier +
                    "\n    epochClock=" + _epochClock +
                    "\n    errorLog=" + _errorLog +
                    "\n    errorHandler=" + _errorHandler +
                    "\n    delegatingErrorHandler=" + _delegatingErrorHandler +
                    "\n    errorCounter=" + _errorCounter +
                    "\n    countedErrorHandler=" + _countedErrorHandler +
                    "\n    clusteredService=" + _clusteredService +
                    "\n    terminationHook=" + _terminationHook +
                    "\n    cycleThresholdNs=" + _cycleThresholdNs +
                    "\n    dutyCycleTracker=" + _dutyCycleTracker +
                    "\n    snapshotDurationThresholdNs=" + _snapshotDurationThresholdNs +
                    "\n    snapshotDurationTracker=" + _snapshotDurationTracker +
                    "\n    markFile=" + _markFile +
                    "\n}";
            }
        }
    }
}

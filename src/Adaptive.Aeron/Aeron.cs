/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.IO;
using System.Text;
using System.Threading;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using Adaptive.Agrona.Concurrent.Errors;
using Adaptive.Agrona.Concurrent.RingBuffer;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Aeron entry point for communicating to the Media Driver for creating <seealso cref="Publication"/>s and <seealso cref="Subscription"/>s.
    /// Use an <seealso cref="Context"/> to configure the Aeron object.
    /// 
    /// A client application requires only one Aeron object per Media Driver.
    /// 
    /// <b>Note:</b> If <seealso cref="Context.ErrorHandler(ErrorHandler)"/> is not set and a <seealso cref="DriverTimeoutException"/>
    /// occurs then the process will face the wrath of <seealso cref="Environment.Exit"/>. See <seealso cref="DEFAULT_ERROR_HANDLER"/>.
    /// 
    /// </summary>
    public sealed class Aeron : IDisposable
    {
        /// <summary>
        /// The Default handler for Aeron runtime exceptions.
        /// When a <seealso cref="DriverTimeoutException"/> is encountered, this handler will
        /// exit the program.
        /// <para>
        /// The error handler can be overridden by supplying an <seealso cref="Context"/> with a custom handler.
        /// 
        /// </para>
        /// </summary>
        /// <seealso cref="Context.ErrorHandler(ErrorHandler)" />
        public static readonly ErrorHandler DEFAULT_ERROR_HANDLER = (throwable) =>
        {
            Console.WriteLine(throwable);

            if (throwable is DriverTimeoutException)
            {
                Console.WriteLine("***");
                Console.WriteLine("***");
                Console.WriteLine("Timeout from the MediaDriver - is it currently running? Exiting.");
                Console.WriteLine("***");
                Console.WriteLine("***");
                Environment.Exit(-1);
            }
        };

        /*
         * Duration in milliseconds for which the client conductor will sleep between duty cycles.
         */
        public static readonly int IdleSleepMs = 16;

        /*
        * Duration in nanoseconds for which the client conductor will sleep between duty cycles.
        */
        public static readonly long IdleSleepNs = NanoUtil.FromMilliseconds(IdleSleepMs);

        /*
         * Default interval between sending keepalive control messages to the driver.
         */
        public static readonly long KeepaliveIntervalNs = NanoUtil.FromMilliseconds(500);

        private static readonly AtomicBoolean _isClosed = new AtomicBoolean(false);
        private readonly long _clientId;
        private readonly ClientConductor _conductor;
        private readonly IRingBuffer _commandBuffer;
        private readonly AgentInvoker _conductorInvoker;
        private readonly AgentRunner _conductorRunner;
        private readonly Context _ctx;

        internal Aeron(Context ctx)
        {
            ctx.Conclude();

            _ctx = ctx;
            _clientId = ctx.ClientId();
            _commandBuffer = ctx.ToDriverBuffer();
            _conductor = new ClientConductor(ctx);

            if (ctx.UseConductorAgentInvoker())
            {
                _conductorInvoker = new AgentInvoker(ctx.ErrorHandler(), null, _conductor);
                _conductorRunner = null;
            }
            else
            {
                _conductorInvoker = null;
                _conductorRunner = new AgentRunner(ctx.IdleStrategy(), ctx.ErrorHandler(), null, _conductor);
            }
        }

        /// <summary>
        /// Create an Aeron instance and connect to the media driver with a default <seealso cref="Context"/>.
        /// 
        /// Threads required for interacting with the media driver are created and managed within the Aeron instance.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the new <seealso cref="Aeron"/> instance connected to the Media Driver. </returns>
        public static Aeron Connect()
        {
            return Connect(new Context());
        }

        /// <summary>
        /// Create an Aeron instance and connect to the media driver.
        /// <para>
        /// Threads required for interacting with the media driver are created and managed within the Aeron instance.
        /// 
        /// </para>
        /// </summary>
        /// <param name="ctx"> for configuration of the client. </param>
        /// <returns> the new <seealso cref="Aeron"/> instance connected to the Media Driver. </returns>
        public static Aeron Connect(Context ctx)
        {
            try
            {
                var aeron = new Aeron(ctx);
                if (ctx.UseConductorAgentInvoker())
                {
                    aeron.ConductorAgentInvoker().Start();
                }
                else
                {
                    AgentRunner.StartOnThread(aeron._conductorRunner, ctx.ThreadFactory());
                }

                return aeron;
            }
            catch (Exception)
            {
                ctx.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Print out the values from <seealso cref="#countersReader()"/> which can be useful for debugging.
        /// </summary>
        ///  <param name="out"> to where the counters get printed. </param>
        public void PrintCounters(StreamWriter @out)
        {
            CountersReader counters = CountersReader();
            counters.ForEach((value, id, label) => @out.WriteLine("{0,3}: {1:} - {2}", id, value, label));
        }


        /// <summary>
        /// Has the client been closed? If not then the CnC file may not be unmapped.
        /// </summary>
        /// <returns> true if the client has been explicitly closed otherwise false. </returns>
        public bool IsClosed()
        {
            return _isClosed.Get();
        }

        /// <summary>
        /// Get the <seealso cref="Context"/> that is used by this client.
        /// </summary>
        /// <returns> the <seealso cref="Context"/> that is use by this client. </returns>
        public Context Ctx()
        {
            return _ctx;
        }

        /// <summary>
        /// Get the client identity that has been allocated for communicating with the media driver.
        /// </summary>
        /// <returns> the client identity that has been allocated for communicating with the media driver. </returns>
        public long ClientId()
        {
            return _clientId;
        }

        /// <summary>
        /// Get the <seealso cref="AgentInvoker"/> for the client conductor.
        /// </summary>
        /// <returns> the <seealso cref="AgentInvoker"/> for the client conductor. </returns>
        public AgentInvoker ConductorAgentInvoker()
        {
            return _conductorInvoker;
        }

        /// <summary>
        /// Clean up and release all Aeron client resources and shutdown conducator thread if not using
        /// <see cref="Context.UseConductorAgentInvoker(bool)"/>.
        /// 
        /// This will close all currently open <see cref="Publication"/>s, <see cref="Subscription"/>s and <see cref="Counter"/>s created
        /// from this client.
        /// </summary>
        public void Dispose()
        {
            if (_isClosed.CompareAndSet(false, true))
            {
                if (null != _conductorRunner)
                {
                    _conductorRunner.Dispose();
                }
                else
                {
                    _conductorInvoker.Dispose();
                }

                _ctx.Dispose();
            }
        }

        /// <summary>
        /// Add a <seealso cref="Publication"/> for publishing messages to subscribers. The publication returned is threadsafe.
        /// </summary>
        /// <param name="channel">  for sending the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> a new <see cref="ConcurrentPublication"/>. </returns>
        public Publication AddPublication(string channel, int streamId)
        {
            return _conductor.AddPublication(channel, streamId);
        }

        /// <summary>
        /// Add an <seealso cref="ExclusivePublication"/> for publishing messages to subscribers from a single thread.
        /// </summary>
        /// <param name="channel">  for sending the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> a new <see cref="ExclusivePublication"/>. </returns>
        public ExclusivePublication AddExclusivePublication(string channel, int streamId)
        {
            return _conductor.AddExclusivePublication(channel, streamId);
        }


        /// <summary>
        /// Add a new <seealso cref="Subscription"/> for subscribing to messages from publishers.
        /// </summary>
        /// <param name="channel">  for receiving the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the <seealso cref="Subscription"/> for the channel and streamId pair. </returns>
        public Subscription AddSubscription(string channel, int streamId)
        {
            return _conductor.AddSubscription(channel, streamId);
        }

        /// <summary>
        /// Add a new <seealso cref="Subscription"/> for subscribing to messages from publishers.
        ///   
        /// This method will override the default handlers from the <seealso cref="Context"/>, i.e.
        /// <seealso cref="Context.AvailableImageHandler(AvailableImageHandler)"/> and
        /// <seealso cref="Context.UnavailableImageHandler(UnavailableImageHandler)"/>. Null values are valid and will
        /// result in no action being taken.
        /// </summary>
        /// <param name="channel">                 for receiving the messages known to the media layer. </param>
        /// <param name="streamId">                within the channel scope. </param>
        /// <param name="availableImageHandler">   called when <seealso cref="Image"/>s become available for consumption. Null is valid if no action is to be taken.</param>
        /// <param name="unavailableImageHandler"> called when <seealso cref="Image"/>s go unavailable for consumption. Null is valid if no action is to be taken.</param>
        /// <returns> the <seealso cref="Subscription"/> for the channel and streamId pair. </returns>
        public Subscription AddSubscription(string channel, int streamId, AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            return _conductor.AddSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
        }

        /// <summary>
        /// Generate the next correlation id that is unique for the connected Media Driver.
        /// 
        /// This is useful generating correlation identifiers for pairing requests with responses in a clients own
        /// application protocol.
        /// 
        /// This method is thread safe and will work across processes that all use the same media driver.
        /// </summary>
        /// <returns> next correlation id that is unique for the Media Driver. </returns>
        public long NextCorrelationId()
        {
            if (_conductor.IsClosed())
            {
                throw new InvalidOperationException("Client is closed");
            }

            return _commandBuffer.NextCorrelationId();
        }

        /// <summary>
        /// Get the <see cref="CountersReader"/> for the Aeron media driver counters.
        /// </summary>
        /// <returns> new <see cref="CountersReader"/> for the Aeron media driver in use.</returns>
        public CountersReader CountersReader()
        {
            if (_conductor.IsClosed())
            {
                throw new InvalidOperationException("Client is closed");
            }

            return _conductor.CountersReader();
        }

        /// <summary>
        /// Allocate a counter on the media driver and return a <seealso cref="Counter"/> for it.
        /// <para>
        /// The counter should be freed by calling <seealso cref="Counter.Dispose()"/>.
        ///   
        /// </para>
        /// </summary>
        /// <param name="typeId">      for the counter. </param>
        /// <param name="keyBuffer">   containing the optional key for the counter. </param>
        /// <param name="keyOffset">   within the keyBuffer at which the key begins. </param>
        /// <param name="keyLength">   of the key in the keyBuffer. </param>
        /// <param name="labelBuffer"> containing the mandatory label for the counter. The label should not be length prefixed. </param>
        /// <param name="labelOffset"> within the labelBuffer at which the label begins. </param>
        /// <param name="labelLength"> of the label in the labelBuffer. </param>
        /// <returns> the newly allocated counter. </returns>
        public Counter AddCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength)
        {
            return _conductor.AddCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset,
                labelLength);
        }

        /// <summary>
        /// Allocate a counter on the media driver and return a <seealso cref="Counter"/> for it.
        /// <para>
        /// The counter should be freed by calling <seealso cref="Counter#close()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="typeId"> for the counter. </param>
        /// <param name="label">  for the counter. It should be US-ASCII. </param>
        /// <returns> the newly allocated counter. </returns>
        /// <seealso cref= org.agrona.concurrent.status.CountersManager#allocate(String, int) </seealso>
        public Counter AddCounter(int typeId, string label)
        {
            return _conductor.AddCounter(typeId, label);
        }

        /// <summary>
        /// This class provides configuration for the <seealso cref="Aeron"/> class via the <seealso cref="Aeron.Connect(Aeron.Context)"/>
        /// method and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
        /// It can also set up error handling as well as application callbacks for image information from the
        /// Media Driver.
        /// 
        /// A number of the properties are for testing and should not be set by end users.
        /// 
        /// <b>Note:</b> Do not reuse instances of the context across different <seealso cref="Aeron"/> clients.
        /// </summary>
        public class Context : IDisposable
        {
            private long _clientId;
            private bool _useConductorAgentInvoker = false;
            private ILock _clientLock;
            private IEpochClock _epochClock;
            private INanoClock _nanoClock;
            private IIdleStrategy _idleStrategy;
            private CopyBroadcastReceiver _toClientBuffer;
            private IRingBuffer _toDriverBuffer;
            private DriverProxy _driverProxy;
            private ILogBuffersFactory _logBuffersFactory;
            private ErrorHandler _errorHandler;
            private AvailableImageHandler _availableImageHandler;
            private UnavailableImageHandler _unavailableImageHandler;
            private AvailableCounterHandler _availableCounterHandler;
            private UnavailableCounterHandler _unavailableCounterHandler;
            private long _keepAliveInterval = KeepaliveIntervalNs;
            private long _interServiceTimeout = 0;
            private FileInfo _cncFile;
            private string _aeronDirectoryName = GetAeronDirectoryName();
            private DirectoryInfo _aeronDirectory;
            private long _driverTimeoutMs = DRIVER_TIMEOUT_MS;
            private MappedByteBuffer _cncByteBuffer;
            private UnsafeBuffer _cncMetaDataBuffer;
            private UnsafeBuffer _countersMetaDataBuffer;
            private UnsafeBuffer _countersValuesBuffer;
            private IThreadFactory _threadFactory = new DefaultThreadFactory();

            /// <summary>
            /// The top level Aeron directory used for communication between a Media Driver and client.
            /// </summary>
            public const string AERON_DIR_PROP_NAME = "aeron.dir";

            /// <summary>
            /// The value of the top level Aeron directory unless overridden by <seealso cref="AeronDirectoryName()"/>
            /// </summary>
            public static readonly string AERON_DIR_PROP_DEFAULT =
                Path.Combine(IoUtil.TmpDirName(), "aeron-" + Environment.UserName);

            /// <summary>
            /// URI used for IPC <seealso cref="Publication"/>s and  <seealso cref="Subscription"/>s
            /// </summary>
            public const string IPC_CHANNEL = "aeron:ipc";

            /// <summary>
            /// URI used for Spy <see cref="Subscription"/>s whereby an outgoing unicast or multicast publication can be spied on
            /// by IPC without receiving it again via the network.
            /// </summary>
            public const string SPY_PREFIX = "aeron-spy:";

            /// <summary>
            /// The address and port used for a UDP channel. For the publisher it is the socket to send to,
            /// for the subscriber it is the socket to receive from.
            /// </summary>
            public const string ENDPOINT_PARAM_NAME = "endpoint";

            /// <summary>
            /// The network interface via which the socket will be routed.
            /// </summary>
            public const string INTERFACE_PARAM_NAME = "interface";

            /// <summary>
            /// Timeout in which the driver is expected to respond.
            /// </summary>
            public const long DRIVER_TIMEOUT_MS = 10000;

            /// <summary>
            /// Initial term id to be used when creating an <seealso cref="ExclusivePublication"/>.
            /// </summary>
            public const string INITIAL_TERM_ID_PARAM_NAME = "init-term-id";

            /// <summary>
            /// Current term id to be used when creating an <seealso cref="ExclusivePublication"/>.
            /// </summary>
            public const string TERM_ID_PARAM_NAME = "term-id";

            /// <summary>
            /// Current term offset to be used when creating an <seealso cref="ExclusivePublication"/>.
            /// </summary>
            public const string TERM_OFFSET_PARAM_NAME = "term-offset";

            /// <summary>
            /// The param name to be used for the term length as a channel URI param.
            /// </summary>
            public const string TERM_LENGTH_PARAM_NAME = "term-length";

            /// <summary>
            /// MTU length parameter name for using as a channel URI param. If this is greater than the network MTU for UDP
            /// then the packet will be fragmented and can amplify the impact of loss.
            /// </summary>
            public const string MTU_LENGTH_PARAM_NAME = "mtu";

            /// <summary>
            /// Time To Live param for a multicast datagram.
            /// </summary>
            public const string TTL_PARAM_NAME = "ttl";

            /// <summary>
            /// The param for the control channel IP address and port for multi-destination-cast semantics.
            /// </summary>
            public const string MDC_CONTROL_PARAM_NAME = "control";

            /// <summary>
            /// Key for the mode of control that such be used for multi-destination-cast semantics.
            /// </summary>
            public const string MDC_CONTROL_MODE_PARAM_NAME = "control-mode";

            /// <summary>
            /// MTU length parameter name for using as a channel URI param.
            /// </summary>
            public const string MTU_LENGTH_URI_PARAM_NAME = "mtu";

            /// <summary>
            /// Key for the mode of control that such be used for multi-destination-cast semantics.
            /// </summary>
            public const string MDC_CONTROL_MODE = "control-mode";

            /// <summary>
            /// Key for the session id for a publication or restricted subscription.
            /// </summary>
            public const string SESSION_ID_PARAM_NAME = "session-id";

            /// <summary>
            /// Key for the linger timeout for a publication to wait around after draining in nanoseconds.
            /// </summary>
            public const string LINGER_PARAM_NAME = "linger";

            /// <summary>
            /// Valid value for <seealso cref="MDC_CONTROL_MODE"/> when manual control is desired.
            /// </summary>
            public const string MDC_CONTROL_MODE_MANUAL = "manual";

            /// <summary>
            /// Valid value for <seealso cref="MDC_CONTROL_MODE_PARAM_NAME"/> when dynamic control is desired. Default value.
            /// </summary>
            public const string MDC_CONTROL_MODE_DYNAMIC = "dynamic";

            /// <summary>
            /// Parameter name for channel URI param to indicate if a subscribed must be reliable or not. Value is boolean.
            /// </summary>
            public const string RELIABLE_STREAM_PARAM_NAME = "reliable";

            /// <summary>
            /// Get the default directory name to be used if <seealso cref="AeronDirectoryName(String)"/> is not set. This will take
            /// the <seealso cref="AERON_DIR_PROP_NAME"/> if set and if not then <seealso cref="AERON_DIR_PROP_DEFAULT"/>.
            /// </summary>
            /// <returns> the default directory name to be used if <seealso cref="AeronDirectoryName(String)"/> is not set. </returns>
            public static string GetAeronDirectoryName()
            {
                return Config.GetProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
            }

            /// <summary>
            /// Conclude the <seealso cref="AeronDirectory"/> so it does not need to keep being recreated.
            /// </summary>
            /// <returns> this for a fluent API. </returns>
            public Context ConcludeAeronDirectory()
            {
                if (null == _aeronDirectory)
                {
                    _aeronDirectory = new DirectoryInfo(_aeronDirectoryName);
                }

                return this;
            }
            
            /// <summary>
            /// Perform a shallow copy of the object.
            /// </summary>
            /// <returns> a shallow copy of the object. </returns>
            public virtual Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// This is called automatically by <seealso cref="Connect()"/> and its overloads.
            /// There is no need to call it from a client application. It is responsible for providing default
            /// values for options that are not individually changed through field setters.
            /// </summary>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context Conclude()
            {
                ConcludeAeronDirectory();

                _cncFile = new FileInfo(Path.Combine(_aeronDirectory.FullName, CncFileDescriptor.CNC_FILE));

                if (null == _clientLock)
                {
                    _clientLock = new ReentrantLock();
                }

                if (_epochClock == null)
                {
                    _epochClock = new SystemEpochClock();
                }

                if (_nanoClock == null)
                {
                    _nanoClock = new SystemNanoClock();
                }

                if (_idleStrategy == null)
                {
                    _idleStrategy = new SleepingIdleStrategy(IdleSleepMs);
                }

                if (CncFile() != null)
                {
                    ConnectToDriver();
                }

                if (_toDriverBuffer == null)
                {
                    _toDriverBuffer =
                        new ManyToOneRingBuffer(CncFileDescriptor.CreateToDriverBuffer(_cncByteBuffer,
                            _cncMetaDataBuffer));
                }

                if (_toClientBuffer == null)
                {
                    _toClientBuffer = new CopyBroadcastReceiver(new BroadcastReceiver(
                        CncFileDescriptor.CreateToClientsBuffer(_cncByteBuffer, _cncMetaDataBuffer)));
                }

                if (CountersMetaDataBuffer() == null)
                {
                    CountersMetaDataBuffer(CncFileDescriptor.CreateCountersMetaDataBuffer(_cncByteBuffer,
                        _cncMetaDataBuffer));
                }

                if (CountersValuesBuffer() == null)
                {
                    CountersValuesBuffer(CncFileDescriptor.CreateCountersValuesBuffer(_cncByteBuffer,
                        _cncMetaDataBuffer));
                }

                _interServiceTimeout = CncFileDescriptor.ClientLivenessTimeout(_cncMetaDataBuffer);

                if (_logBuffersFactory == null)
                {
                    _logBuffersFactory = new MappedLogBuffersFactory();
                }

                if (_errorHandler == null)
                {
                    _errorHandler = DEFAULT_ERROR_HANDLER;
                }

                if (_availableImageHandler == null)
                {
                    _availableImageHandler = image => { };
                }

                if (_unavailableImageHandler == null)
                {
                    _unavailableImageHandler = image => { };
                }

                if (null == _driverProxy)
                {
                    _clientId = _toDriverBuffer.NextCorrelationId();
                    _driverProxy = new DriverProxy(ToDriverBuffer(), _clientId);
                }

                return this;
            }

            /// <summary>
            /// Get the client identity that has been allocated for communicating with the media driver.
            /// </summary>
            /// <returns> the client identity that has been allocated for communicating with the media driver.</returns>
            public long ClientId()
            {
                return _clientId;
            }

            /// <summary>
            /// Get the command and control file.
            /// </summary>
            /// <returns> The command and control file. </returns>
            public FileInfo CncFile()
            {
                return _cncFile;
            }

            /// <summary>
            /// Should an <see cref="AgentInvoker"/> be used for running the <see cref="ClientConductor"/> rather than run it on
            /// a thread with an <see cref="AgentRunner"/>
            /// </summary>
            /// <param name="useConductorAgentInvoker"> use <see cref="AgentInvoker"/> for running the <see cref="ClientConductor"/></param>
            /// <returns> this for a fluent API.</returns>
            public Context UseConductorAgentInvoker(bool useConductorAgentInvoker)
            {
                _useConductorAgentInvoker = useConductorAgentInvoker;
                return this;
            }

            /// <summary>
            /// Should an <see cref="AgentInvoker"/> be used for running the <see cref="ClientConductor"/> rather than run it on
            /// a thread with an <see cref="AgentRunner"/>
            /// </summary>
            /// <returns> true if the <see cref="ClientConductor"/> will be run with an <see cref="AgentInvoker"/> otherwise false.</returns>
            public bool UseConductorAgentInvoker()
            {
                return _useConductorAgentInvoker;
            }

            /// <summary>
            /// The <see cref="ILock"/> that is used to provide mutual exclusion in the Aeron client.
            /// 
            /// If the <see cref="UseConductorAgentInvoker(bool)"/> is set and only one thread accesses the client
            /// then the lock can be set to <see cref="NoOpLock"/> to elide the lock overhead.
            /// 
            /// </summary>
            /// <param name="lock"> that is used to provide mutual exclusion in the Aeron client.</param>
            /// <returns> this for a fluent API.</returns>
            public Context ClientLock(ILock @lock)
            {
                _clientLock = @lock;
                return this;
            }

            /// <summary>
            /// Get the <see cref="ILock"/> that is used to provide mutual exclusion in the Aeron client.
            /// </summary>
            /// <returns> the <see cref="ILock"/> that is used to provide mutual exclusion in the Aeron client.</returns>
            public ILock ClientLock()
            {
                return _clientLock;
            }

            /// <summary>
            /// Set the <seealso cref="IEpochClock"/> to be used for tracking wall clock time when interacting with the driver.
            /// </summary>
            /// <param name="clock"> <seealso cref="IEpochClock"/> to be used for tracking wall clock time when interacting with the driver. </param>
            /// <returns> this Aeron.Context for method chaining </returns>
            public Context EpochClock(IEpochClock clock)
            {
                _epochClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IEpochClock"/> used by the client for the epoch time in milliseconds.
            /// </summary>
            /// <returns> the <seealso cref="IEpochClock"/> used by the client for the epoch time in milliseconds. </returns>
            public IEpochClock EpochClock()
            {
                return _epochClock;
            }


            /// <summary>
            /// Set the <seealso cref="NanoClock"/> to be used for tracking high resolution time.
            /// </summary>
            /// <param name="clock"> <seealso cref="NanoClock"/> to be used for tracking high resolution time. </param>
            /// <returns> this Aeron.Context for method chaining </returns>
            public Context NanoClock(INanoClock clock)
            {
                _nanoClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="INanoClock"/> to be used for tracking high resolution time.
            /// </summary>
            /// <returns> the <seealso cref="INanoClock"/> to be used for tracking high resolution time. </returns>
            public INanoClock NanoClock()
            {
                return _nanoClock;
            }

            /// <summary>
            /// Provides an IdleStrategy for the thread responsible for communicating with the Aeron Media Driver.
            /// </summary>
            /// <param name="idleStrategy"> Thread idle strategy for communication with the Media Driver. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context IdleStrategy(IIdleStrategy idleStrategy)
            {
                _idleStrategy = idleStrategy;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IIdleStrategy"/> employed by the client conductor thread.
            /// </summary>
            /// <returns> the <seealso cref="IIdleStrategy"/> employed by the client conductor thread. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return _idleStrategy;
            }

            /// <summary>
            /// This method is used for testing and debugging.
            /// </summary>
            /// <param name="toClientBuffer"> Injected CopyBroadcastReceiver </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            internal Context ToClientBuffer(CopyBroadcastReceiver toClientBuffer)
            {
                _toClientBuffer = toClientBuffer;
                return this;
            }

            /// <summary>
            /// The buffer used for communicating from the media driver to the Aeron client.
            /// </summary>
            /// <returns> the buffer used for communicating from the media driver to the Aeron client. </returns>
            public CopyBroadcastReceiver ToClientBuffer()
            {
                return _toClientBuffer;
            }

            /// <summary>
            /// This method is used for testing and debugging.
            /// </summary>
            /// <param name="toDriverBuffer"> Injected RingBuffer. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            internal Context ToDriverBuffer(IRingBuffer toDriverBuffer)
            {
                _toDriverBuffer = toDriverBuffer;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IRingBuffer"/> used for sending commands to the media driver.
            /// </summary>
            /// <returns> the <seealso cref="IRingBuffer"/> used for sending commands to the media driver. </returns>
            public IRingBuffer ToDriverBuffer()
            {
                return _toDriverBuffer;
            }

            /// <summary>
            /// Set the proxy for communicating with the media driver.
            /// </summary>
            /// <param name="driverProxy"> for communicating with the media driver. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            internal Context DriverProxy(DriverProxy driverProxy)
            {
                _driverProxy = driverProxy;
                return this;
            }

            /// <summary>
            /// Get the proxy for communicating with the media driver.
            /// </summary>
            /// <returns> the proxy for communicating with the media driver. </returns>
            public DriverProxy DriverProxy()
            {
                return _driverProxy;
            }


            /// <summary>
            /// This method is used for testing and debugging.
            /// </summary>
            /// <param name="logBuffersFactory"> Injected LogBuffersFactory </param>
            /// <returns> this for a fluent API. </returns>
            internal Context LogBuffersFactory(ILogBuffersFactory logBuffersFactory)
            {
                _logBuffersFactory = logBuffersFactory;
                return this;
            }

            /// <summary>
            /// Get the factory for making log buffers.
            /// </summary>
            /// <returns> the factory for making log buffers. </returns>
            public ILogBuffersFactory LogBuffersFactory()
            {
                return _logBuffersFactory;
            }


            /// <summary>
            /// Handle Aeron exceptions in a callback method. The default behavior is defined by
            /// <seealso cref="Aeron.DEFAULT_ERROR_HANDLER"/>. This is the error handler which will be used if an error occurs
            /// during the callback for poll operations such as <seealso cref="Subscription.Poll(FragmentHandler, int)"/>.
            /// 
            /// The error handler can be reset after <seealso cref="Aeron.Connect()"/> and the latest version will always be used
            /// so that the boot strapping process can be performed such as replacing the default one with a
            /// <seealso cref="CountedErrorHandler"/>.
            /// <param name="errorHandler"> Method to handle objects of type Throwable. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            /// <seealso cref="DriverTimeoutException" />
            /// <seealso cref="RegistrationException" />
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                _errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the error handler that will be called for errors reported back from the media driver.
            /// </summary>
            /// <returns> the error handler that will be called for errors reported back from the media driver. </returns>
            public ErrorHandler ErrorHandler()
            {
                return _errorHandler;
            }

            /// <summary>
            /// Setup a default callback for when an <seealso cref="Image"/> is available.
            /// </summary>
            /// <param name="handler"> Callback method for handling available image notifications. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context AvailableImageHandler(AvailableImageHandler handler)
            {
                _availableImageHandler = handler;
                return this;
            }

            /// <summary>
            /// Get the default callback handler for notifying when <seealso cref="Image"/>s become available.
            /// </summary>
            /// <returns> the callback handler for notifying when <seealso cref="Image"/>s become available. </returns>
            public AvailableImageHandler AvailableImageHandler()
            {
                return _availableImageHandler;
            }

            /// <summary>
            /// Setup a default callback for when an <seealso cref="Image"/> is unavailable.
            /// </summary>
            /// <param name="handler"> Callback method for handling unavailable image notifications. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context UnavailableImageHandler(UnavailableImageHandler handler)
            {
                _unavailableImageHandler = handler;
                return this;
            }

            /// <summary>
            /// Get the callback handler for when an <seealso cref="Image"/> is unavailable.
            /// </summary>
            /// <returns> the callback handler for when an <seealso cref="Image"/> is unavailable. </returns>
            public UnavailableImageHandler UnavailableImageHandler()
            {
                return _unavailableImageHandler;
            }

            /// <summary>
            /// Setup a callback for when a counter is available.
            /// </summary>
            /// <param name="handler"> to be called for handling available counter notifications. </param>
            /// <returns> this Aeron.Context for fluent API. </returns>
            public Context AvailableCounterHandler(AvailableCounterHandler handler)
            {
                _availableCounterHandler = handler;
                return this;
            }

            /// <summary>
            /// Get the callback handler for when a counter is available.
            /// </summary>
            /// <returns> the callback handler for when a counter is available. </returns>
            public AvailableCounterHandler AvailableCounterHandler()
            {
                return _availableCounterHandler;
            }

            /// <summary>
            /// Setup a callback for when a counter is unavailable.
            /// </summary>
            /// <param name="handler"> to be called for handling unavailable counter notifications. </param>
            /// <returns> this Aeron.Context for fluent API. </returns>
            public Context UnavailableCounterHandler(UnavailableCounterHandler handler)
            {
                _unavailableCounterHandler = handler;
                return this;
            }

            /// <summary>
            /// Get the callback handler for when a counter is unavailable.
            /// </summary>
            /// <returns> the callback handler for when a counter is unavailable. </returns>
            public UnavailableCounterHandler UnavailableCounterHandler()
            {
                return _unavailableCounterHandler;
            }


            /// <summary>
            /// Get the buffer containing the counter meta data. These counters are R/W for the driver, read only for all
            /// other users.
            /// </summary>
            /// <returns> The buffer storing the counter meta data. </returns>
            public UnsafeBuffer CountersMetaDataBuffer()
            {
                return _countersMetaDataBuffer;
            }

            /// <summary>
            /// Set the buffer containing the counter meta data. Testing/internal purposes only.
            /// </summary>
            /// <param name="countersMetaDataBuffer"> The new counter meta data buffer. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CountersMetaDataBuffer(UnsafeBuffer countersMetaDataBuffer)
            {
                _countersMetaDataBuffer = countersMetaDataBuffer;
                return this;
            }


            /// <summary>
            /// Get the buffer containing the counters. These counters are R/W for the driver, read only for all other users.
            /// </summary>
            /// <returns> The buffer storing the counters. </returns>
            public UnsafeBuffer CountersValuesBuffer()
            {
                return _countersValuesBuffer;
            }

            /// <summary>
            /// Set the buffer containing the counters. Testing/internal purposes only.
            /// </summary>
            /// <param name="countersValuesBuffer"> The new counters buffer. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CountersValuesBuffer(UnsafeBuffer countersValuesBuffer)
            {
                _countersValuesBuffer = countersValuesBuffer;
                return this;
            }


            /// <summary>
            /// Set the interval in nanoseconds for which the client will perform keep-alive operations.
            /// </summary>
            /// <param name="value"> the interval in nanoseconds for which the client will perform keep-alive operations. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context KeepAliveInterval(long value)
            {
                _keepAliveInterval = value;
                return this;
            }

            /// <summary>
            /// Get the interval in nanoseconds for which the client will perform keep-alive operations.
            /// </summary>
            /// <returns> the interval in nanoseconds for which the client will perform keep-alive operations. </returns>
            public long KeepAliveInterval()
            {
                return _keepAliveInterval;
            }

            /// <summary>
            /// Set the amount of time, in milliseconds, that this client will wait until it determines the
            /// Media Driver is unavailable. When this happens a
            /// <seealso cref="DriverTimeoutException"/> will be generated for the error handler.
            /// </summary>
            /// <param name="driverTimeoutMs"> Number of milliseconds. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Agrona.ErrorHandler" />
            public Context DriverTimeoutMs(long driverTimeoutMs)
            {
                _driverTimeoutMs = driverTimeoutMs;
                return this;
            }

            /// <summary>
            /// Get the driver timeout in milliseconds.
            /// </summary>
            /// <returns> driver timeout in milliseconds. </returns>
            public long DriverTimeoutMs()
            {
                return _driverTimeoutMs;
            }

            /// <summary>
            /// Set the timeout between service calls the to <seealso cref="ClientConductor"/> duty cycles.
            /// </summary>
            /// <param name="interServiceTimeout"> the timeout (ns) between service calls the to <seealso cref="ClientConductor"/> duty cycle. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            internal Context InterServiceTimeout(long interServiceTimeout)
            {
                _interServiceTimeout = interServiceTimeout;
                return this;
            }

            /// <summary>
            /// Return the timeout between service calls to the duty cycle for the client.
            /// 
            /// When exceeded, <seealso cref="Agrona.ErrorHandler"/> will be called and the active <seealso cref="Publication"/>s and <seealso cref="Image"/>s
            /// closed.
            /// 
            /// This value is controlled by the driver and included in the CnC file. It can be configured by adjusting
            /// the aeron.client.liveness.timeout property on the media driver.
            /// </summary>
            /// <returns> the timeout between service calls in nanoseconds. </returns>
            public long InterServiceTimeout()
            {
                return _interServiceTimeout;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the client and Media Driver, and
            /// the location of the data buffers.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return _aeronDirectoryName;
            }

            /// <summary>
            /// Get the directory in which the aeron config files are stored.
            ///    
            /// This is valid after a call to <seealso cref="Conclude()"/>.
            /// </summary>
            /// <returns> the directory in which the aeron config files are stored.
            /// </returns>
            /// 
            /// <seealso cref="AeronDirectoryName()"></seealso>
            public DirectoryInfo AeronDirectory()
            {
                return _aeronDirectory;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the client and Media Driver, and the location
            /// of the data buffers.
            /// Check this setting if there is a DriverTimeoutException
            /// The default path for communicating between the driver and client is based on the process owner's temp directory. %localappdata%\temp\aeron-[username]
            /// </summary>
            /// <param name="dirName"> New top level Aeron directory. </param>
            /// <returns> this Object for method chaining. </returns>
            public Context AeronDirectoryName(string dirName)
            {
                _aeronDirectoryName = dirName;
                return this;
            }

            /// <summary>
            /// Specify the thread factory to use when starting the conductor thread.
            /// </summary>
            /// <param name="threadFactory"> thread factory to construct the thread.</param>
            /// <returns> this for a fluent API.</returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                _threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// The thread factory to be use to construct the conductor thread
            /// </summary>
            /// <returns>the specified thread factory of <see cref="DefaultThreadFactory"/> if none is provided.</returns>
            public IThreadFactory ThreadFactory()
            {
                return _threadFactory;
            }

            /// <summary>
            /// Clean up all resources that the client uses to communicate with the Media Driver.
            /// </summary>
            public void Dispose()
            {
                IoUtil.Unmap(_cncByteBuffer);
                _cncByteBuffer = null;

                _cncMetaDataBuffer?.Dispose();
                _countersMetaDataBuffer?.Dispose();
                _countersValuesBuffer?.Dispose();
                _cncByteBuffer?.Dispose();
            }

            private void ConnectToDriver()
            {
                long startTimeMs = _epochClock.Time();
                FileInfo cncFile = CncFile();

                while (true)
                {
                    while (!cncFile.Exists)
                    {
                        if (_epochClock.Time() > (startTimeMs + _driverTimeoutMs))
                        {
                            throw new DriverTimeoutException("CnC file not found: " + cncFile.FullName);
                        }

                        Sleep(16);
                    }

                    _cncByteBuffer = IoUtil.MapExistingFile(CncFile(), CncFileDescriptor.CNC_FILE);
                    _cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(_cncByteBuffer);

                    int cncVersion;
                    while (0 == (cncVersion = _cncMetaDataBuffer.GetIntVolatile(CncFileDescriptor.CncVersionOffset(0))))
                    {
                        if (_epochClock.Time() > (startTimeMs + DriverTimeoutMs()))
                        {
                            throw new DriverTimeoutException("CnC file is created but not initialised.");
                        }

                        Sleep(1);
                    }

                    if (CncFileDescriptor.CNC_VERSION != cncVersion)
                    {
                        throw new InvalidOperationException("CnC file version not supported: version=" + cncVersion);
                    }

                    ManyToOneRingBuffer ringBuffer =
                        new ManyToOneRingBuffer(
                            CncFileDescriptor.CreateToDriverBuffer(_cncByteBuffer, _cncMetaDataBuffer));

                    while (0 == ringBuffer.ConsumerHeartbeatTime())
                    {
                        if (_epochClock.Time() > (startTimeMs + DriverTimeoutMs()))
                        {
                            throw new DriverTimeoutException("No driver heartbeat detected.");
                        }

                        Sleep(1);
                    }

                    long timeMs = _epochClock.Time();
                    if (ringBuffer.ConsumerHeartbeatTime() < (timeMs - DriverTimeoutMs()))
                    {
                        if (timeMs > (startTimeMs + DriverTimeoutMs()))
                        {
                            throw new DriverTimeoutException("No driver heartbeat detected.");
                        }

                        IoUtil.Unmap(_cncByteBuffer);
                        _cncByteBuffer = null;
                        _cncMetaDataBuffer = null;

                        Sleep(100);
                        continue;
                    }

                    if (null == _toDriverBuffer)
                    {
                        _toDriverBuffer = ringBuffer;
                    }

                    break;
                }
            }

            public void DeleteAeronDirectory()
            {
                IoUtil.Delete(_aeronDirectory, false);
            }

            /// <summary>
            /// Map the CnC file if it exists.
            /// </summary>
            /// <param name="logProgress"> for feedback</param>
            /// <returns> a new mapping for the file if it exists otherwise null</returns>
            public MappedByteBuffer MapExistingCncFile(Action<string> logProgress)
            {
                FileInfo cncFile = new FileInfo(Path.Combine(_aeronDirectory.FullName, CncFileDescriptor.CNC_FILE));

                if (cncFile.Exists)
                {
                    if (null != logProgress)
                    {
                        logProgress("INFO: Aeron CnC file " + cncFile + "exists");
                    }

                    return IoUtil.MapExistingFile(cncFile, CncFileDescriptor.CNC_FILE);
                }

                return null;
            }

            /// <summary>
            /// Is a media driver active in the given direction?
            /// </summary>
            /// <param name="directory"> to check</param>
            /// <param name="driverTimeoutMs"> for the driver liveness check.</param>
            /// <param name="logger"> for feedback as liveness checked.</param>
            /// <returns> true if a driver is active or false if not.</returns>
            public static bool IsDriverActive(DirectoryInfo directory, long driverTimeoutMs, Action<string> logger)
            {
                FileInfo cncFile = new FileInfo(Path.Combine(directory.FullName, CncFileDescriptor.CNC_FILE));

                if (cncFile.Exists)
                {
                    logger("INFO: Aeron CnC file " + cncFile + " exists");

                    var cncByteBuffer = IoUtil.MapExistingFile(cncFile, "CnC file");
                    try
                    {
                        return IsDriverActive(driverTimeoutMs, logger, cncByteBuffer);
                    }
                    finally
                    {
                        IoUtil.Unmap(cncByteBuffer);
                    }
                }

                return false;
            }

            /// <summary>
            /// Is a media driver active in the current Aeron directory?
            /// </summary>
            /// <param name="driverTimeoutMs"> for the driver liveness check.</param>
            /// <param name="logger"> for feedback as liveness checked.</param>
            /// <returns> true if a driver is active or false if not.</returns>
            public bool IsDriverActive(long driverTimeoutMs, Action<string> logger)
            {
                var cncByteBuffer = MapExistingCncFile(logger);
                try
                {
                    return IsDriverActive(driverTimeoutMs, logger, cncByteBuffer);
                }
                finally
                {
                    IoUtil.Unmap(cncByteBuffer);
                }
            }

            /// <summary>
            /// Is a media driver active in the current mapped CnC buffer?
            /// </summary>
            /// <param name="driverTimeoutMs"> for the driver liveness check.</param>
            /// <param name="logger">     for feedback as liveness checked.</param>
            /// <param name="cncByteBuffer">   for the existing CnC file.</param>
            /// <returns> true if a driver is active or false if not.</returns>
            public static bool IsDriverActive(long driverTimeoutMs, Action<string> logger,
                MappedByteBuffer cncByteBuffer)
            {
                if (null == cncByteBuffer)
                {
                    return false;
                }

                UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);

                long startTimeMs = UnixTimeConverter.CurrentUnixTimeMillis();
                int cncVersion;
                while (0 == (cncVersion = cncMetaDataBuffer.GetIntVolatile(CncFileDescriptor.CncVersionOffset(0))))
                {
                    if (UnixTimeConverter.CurrentUnixTimeMillis() > (startTimeMs + driverTimeoutMs))
                    {
                        throw new DriverTimeoutException("CnC file is created but not initialised.");
                    }

                    Sleep(1);
                }

                if (CncFileDescriptor.CNC_VERSION != cncVersion)
                {
                    throw new InvalidOperationException("Aeron CnC version does not match: version=" + cncVersion +
                                                        " required=" + CncFileDescriptor.CNC_VERSION);
                }

                ManyToOneRingBuffer toDriverBuffer =
                    new ManyToOneRingBuffer(CncFileDescriptor.CreateToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                long timestamp = toDriverBuffer.ConsumerHeartbeatTime();
                long now = DateTime.Now.ToFileTimeUtc();
                long diff = now - timestamp;

                logger("INFO: Aeron toDriver consumer heartbeat is " + diff + "ms old");

                return diff <= driverTimeoutMs;
            }

            /// <summary>
            /// Read the error log to a given <seealso cref="StreamWriter"/>
            /// </summary>
            /// <param name="writer"> to write the error log contents to. </param>
            /// <returns> the number of observations from the error log </returns>
            public int SaveErrorLog(StreamWriter writer)
            {
                MappedByteBuffer cncByteBuffer = MapExistingCncFile(null);

                try
                {
                    return SaveErrorLog(writer, cncByteBuffer);
                }
                finally
                {
                    IoUtil.Unmap(cncByteBuffer);
                }
            }

            /// <summary>
            /// Read the error log to a given <seealso cref="StreamWriter"/>
            /// </summary>
            /// <param name="writer"> to write the error log contents to. </param>
            /// <returns> the number of observations from the error log </returns>
            public int SaveErrorLog(StreamWriter writer, MappedByteBuffer cncByteBuffer)
            {
                if (null == cncByteBuffer)
                {
                    return 0;
                }


                UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);
                int cncVersion = cncMetaDataBuffer.GetInt(CncFileDescriptor.CncVersionOffset(0));

                if (CncFileDescriptor.CNC_VERSION != cncVersion)
                {
                    throw new InvalidOperationException(
                        "Aeron CnC version does not match: required=" + CncFileDescriptor.CNC_VERSION + " version=" +
                        cncVersion);
                }

                UnsafeBuffer buffer = CncFileDescriptor.CreateErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);

                void ErrorConsumer(int count, long firstTimestamp, long lastTimestamp, string ex)
                    => FormatError(writer, count, firstTimestamp, lastTimestamp, ex);

                var distinctErrorCount = ErrorLogReader.Read(buffer, ErrorConsumer);

                writer.WriteLine();
                writer.WriteLine("{0} distinct errors observed.", distinctErrorCount);

                return distinctErrorCount;
            }

            private static void FormatError(
                TextWriter writer,
                int observationCount,
                long firstObservationTimestamp,
                long lastObservationTimestamp,
                string encodedException)
            {
                writer.WriteLine(
                    $"***{Environment.NewLine}{observationCount} observations from {new DateTime(firstObservationTimestamp)} " +
                    $"to {new DateTime(lastObservationTimestamp)} " +
                    $"for:{Environment.NewLine} {encodedException}");
            }
        }

        internal static void Sleep(int durationMs)
        {
            try
            {
                Thread.Sleep(durationMs);
            }
            catch (ThreadInterruptedException ignore)
            {
                // Java version runs Thread.interrupted() here to clear the interrupted state. Can't find a C# equivalent.
            }
        }
    }
}

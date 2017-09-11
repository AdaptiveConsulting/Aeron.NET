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

        /*
         * Default interval that if exceeded between duty cycles the conductor will consider itself a zombie and suicide.
         */
        public static readonly long InterServiceTimeoutNs = NanoUtil.FromSeconds(10);

        /*
         * Timeout after which if no status messages have been received then a publication is considered not connected.
         */
        public const long PublicationConnectionTimeoutMs = 5000;

        private readonly long _clientId;
        private readonly ILock _clientLock;
        private readonly Context _ctx;
        private readonly ClientConductor _conductor;
        private readonly AgentRunner _conductorRunner;
        private readonly AgentInvoker _conductorInvoker;
        private readonly IRingBuffer _commandBuffer;

        internal Aeron(Context ctx)
        {
            ctx.Conclude();

            _ctx = ctx;
            _clientId = ctx.ClientId();
            _clientLock = ctx.ClientLock();
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
                    aeron.Start(ctx.ThreadFactory());
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
        /// Clean up and release all Aeron internal resources and shutdown threads.
        /// </summary>
        public void Dispose()
        {
            _clientLock.Lock();
            try
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
            finally
            {
                _clientLock.Unlock();
            }
        }

        /// <summary>
        /// Add a <seealso cref="Publication"/> for publishing messages to subscribers.
        /// </summary>
        /// <param name="channel">  for receiving the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the new Publication. </returns>
        public Publication AddPublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                return _conductor.AddPublication(channel, streamId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        /// <summary>
        /// Add an <seealso cref="ExclusivePublication"/> for publishing messages to subscribers from a single thread.
        /// </summary>
        /// <param name="channel">  for receiving the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the new Publication. </returns>
        public ExclusivePublication AddExclusivePublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                return _conductor.AddExclusivePublication(channel, streamId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }


        /// <summary>
        /// Add a new <seealso cref="Subscription"/> for subscribing to messages from publishers.
        /// </summary>
        /// <param name="channel">  for receiving the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the <seealso cref="Subscription"/> for the channel and streamId pair. </returns>
        public Subscription AddSubscription(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                return _conductor.AddSubscription(channel, streamId);
            }
            finally
            {
                _clientLock.Unlock();
            }
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
            _clientLock.Lock();
            try
            {
                return _conductor.AddSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
            }
            finally
            {
                _clientLock.Unlock();
            }
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
        /// Create and returns a <see cref="CountersReader"/> for the Aeron media driver counters.
        /// </summary>
        /// <returns> new <see cref="CountersReader"/> for the Aeron media driver in use.</returns>
        public CountersReader CountersReader()
        {
            if (_conductor.IsClosed())
            {
                throw new InvalidOperationException("Client is closed");
            }
            
            return new CountersReader(_ctx.CountersMetaDataBuffer(), _ctx.CountersValuesBuffer(), Encoding.ASCII);
        }

        private Aeron Start(IThreadFactory threadFactory)
        {
            AgentRunner.StartOnThread(_conductorRunner, threadFactory);

            return this;
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
            private long _keepAliveInterval = KeepaliveIntervalNs;
            private long _interServiceTimeout = 0;
            private long _publicationConnectionTimeout = PublicationConnectionTimeoutMs;
            private FileInfo _cncFile;
            private string _aeronDirectoryName;
            private DirectoryInfo _aeronDirectory;
            private long _driverTimeoutMs = DRIVER_TIMEOUT_MS;
            private MappedByteBuffer _cncByteBuffer;
            private UnsafeBuffer _cncMetaDataBuffer;
            private UnsafeBuffer _countersMetaDataBuffer;
            private UnsafeBuffer _countersValuesBuffer;
            private MapMode _imageMapMode = MapMode.ReadOnly;
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
            /// MTU length parameter name for using as a channel URI param.
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
            /// Valid value for <seealso cref="MDC_CONTROL_MODE"/> when manual control is desired.
            /// </summary>
            public const string MDC_CONTROL_MODE_MANUAL = "manual";

            /// <summary>
            /// Parameter name for channel URI param to indicate if a subscribed must be reliable or not. Value is boolean.
            /// </summary>
            public const string RELIABLE_STREAM_PARAM_NAME = "reliable";

            public Context()
            {
                _aeronDirectoryName = Config.GetProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
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

                if (0 == _interServiceTimeout)
                {
                    _interServiceTimeout = CncFileDescriptor.ClientLivenessTimeout(_cncMetaDataBuffer);
                }
                else
                {
                    _interServiceTimeout = InterServiceTimeoutNs;
                }

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
            public Context ToClientBuffer(CopyBroadcastReceiver toClientBuffer)
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
            public Context ToDriverBuffer(IRingBuffer toDriverBuffer)
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
            public Context DriverProxy(DriverProxy driverProxy)
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
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context LogBuffersFactory(ILogBuffersFactory logBuffersFactory)
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
            /// <seealso cref="Aeron.DEFAULT_ERROR_HANDLER"/>.
            /// </summary>
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
            /// <returns> this Object for method chaining. </returns>
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
            /// <returns> this Object for method chaining. </returns>
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
            /// <returns> this Aeron.Context for method chaining. </returns>
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
            /// <param name="interServiceTimeout"> the timeout (ms) between service calls the to <seealso cref="ClientConductor"/> duty cycles. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context InterServiceTimeout(long interServiceTimeout)
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
            /// This value is controlled by the driver and included in the CnC file.
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
            /// </summary>
            /// <param name="dirName"> New top level Aeron directory. </param>
            /// <returns> this Object for method chaining. </returns>
            public Context AeronDirectoryName(string dirName)
            {
                _aeronDirectoryName = dirName;
                return this;
            }

            /// <summary>
            /// Set the amount of time, in milliseconds, that this client will use to determine if a <seealso cref="Publication"/>
            /// has active subscribers or not.
            /// </summary>
            /// <param name="value"> number of milliseconds. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context PublicationConnectionTimeout(long value)
            {
                _publicationConnectionTimeout = value;
                return this;
            }

            /// <summary>
            /// The file memory mapping mode for <see cref="Image"/>s
            /// </summary>
            /// <param name="imageMapMode"> file memory mapping mode for <see cref="Image"/>s</param>
            /// <returns> this for a fluent API</returns>
            public Context ImageMapMode(MapMode imageMapMode)
            {
                _imageMapMode = imageMapMode;
                return this;
            }

            /// <summary>
            /// The file memory mapping mode for <seealso cref="Image"/>s.
            /// </summary>
            /// <returns> the file memory mapping mode for <seealso cref="Image"/>s. </returns>
            public MapMode ImageMapMode()
            {
                return _imageMapMode;
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
            /// Return the timeout, in milliseconds, that this client will use to determine if a <seealso cref="Publication"/>
            /// has active subscribers or not.
            /// </summary>
            /// <returns> timeout in milliseconds. </returns>
            public long PublicationConnectionTimeout()
            {
                return _publicationConnectionTimeout;
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
            public static bool IsDriverActive(long driverTimeoutMs, Action<string> logger, MappedByteBuffer cncByteBuffer)
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
                        "Aeron CnC version does not match: required=" + CncFileDescriptor.CNC_VERSION + " version=" + cncVersion);
                }

                UnsafeBuffer buffer = CncFileDescriptor.CreateErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
                var distinctErrorCount = ErrorLogReader.Read(
                    buffer,
                    (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) =>
                        writer.WriteLine(
                            $"***{Environment.NewLine}{observationCount} observations from {new DateTime(firstObservationTimestamp)} to {new DateTime(lastObservationTimestamp)} for:{Environment.NewLine} {encodedException}"));

                writer.WriteLine();
                writer.WriteLine("{0} distinct errors observed.", distinctErrorCount);

                return distinctErrorCount;
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
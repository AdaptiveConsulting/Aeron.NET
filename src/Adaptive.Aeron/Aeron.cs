﻿/*
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
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using Adaptive.Agrona.Concurrent.RingBuffer;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Aeron entry point for communicating to the Media Driver for creating <seealso cref="Publication"/>s and <seealso cref="Subscription"/>s.
    /// Use an <seealso cref="Context"/> to configure the Aeron object.
    /// <para>
    /// A client application requires only one Aeron object per Media Driver.
    /// </para>
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
                Console.WriteLine("Timeout from the Media Driver - is it currently running? Exiting.");
                Console.WriteLine("***");
                Console.WriteLine("***");
                Environment.Exit(-1);
            }
        };

        /*
         * Duration in nanoseconds for which the client conductor will sleep between duty cycles.
         */
        private const int IdleSleepMs = 10;

        /*
         * Default interval between sending keepalive control messages to the driver.
         */
        private static readonly long KeepaliveIntervalNs = NanoUtil.FromMilliseconds(500);

        /*
         * Default interval that if exceeded between duty cycles the conductor will consider itself a zombie and suicide.
         */
        private static readonly long InterServiceTimeoutNs = NanoUtil.FromSeconds(10);

        /*
         * Timeout after which if no status messages have been receiver then a publication is considered not connected.
         */
        private const long PublicationConnectionTimeoutMs = 5000;

        private bool _isClosed = false;
        private readonly ClientConductor _conductor;
        private readonly AgentRunner _conductorRunner;
        private readonly Context _ctx;

        internal Aeron(Context ctx)
        {
            _ctx = ctx.Conclude();
            _conductor = ctx.CreateClientConductor();
            _conductorRunner = ctx.CreateConductorRunner(_conductor);
        }

        /// <summary>
        /// Create an Aeron instance and connect to the media driver with a default <seealso cref="Context"/>.
        /// <para>
        /// Threads required for interacting with the media driver are created and managed within the Aeron instance.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the new <seealso cref="Aeron"/> instance connected to the Media Driver. </returns>
        public static Aeron Connect()
        {
            return new Aeron(new Context()).Start();
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
            return new Aeron(ctx).Start();
        }

        /// <summary>
        /// Clean up and release all Aeron internal resources and shutdown threads.
        /// </summary>
        public void Dispose()
        {
            lock (_conductor)
            {
                if (!_isClosed)
                {
                    _isClosed = true;
                    _conductorRunner.Dispose();
                    _ctx.Dispose();
                }
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
            lock (_conductor)
            {
                if (_isClosed)
                {
                    throw new InvalidOperationException("Aeron client is closed");
                }
                return _conductor.AddPublication(channel, streamId);
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
            lock (_conductor)
            {
                if (_isClosed)
                {
                    throw new InvalidOperationException("Aeron client is closed");
                }
                return _conductor.AddSubscription(channel, streamId);
            }
        }

        /// <summary>
        /// Create and returns a <see cref="CountersReader"/> for the Aeron media driver counters.
        /// </summary>
        /// <returns> new <see cref="CountersReader"/> for the Aeron media driver in use.</returns>
        public CountersReader CountersReader()
        {
            lock (_conductor)
            {
                if (_isClosed)
                {
                    throw new InvalidOperationException("Aeron client is closed");
                }
                return new CountersReader(_ctx.CountersMetaDataBuffer(), _ctx.CountersValuesBuffer());
            }
        }

        private Aeron Start()
        {
            AgentRunner.StartOnThread(_conductorRunner, _ctx.ThreadFactory());
            return this;
        }

        /// <summary>
        /// This class provides configuration for the <seealso cref="Aeron"/> class via the <seealso cref="Aeron#connect(Aeron.Context)"/>
        /// method and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
        /// It can also set up error handling as well as application callbacks for image information from the
        /// Media Driver.
        /// </summary>
        public class Context : IDisposable
        {
            private IEpochClock _epochClock;
            private INanoClock _nanoClock;
            private IIdleStrategy _idleStrategy;
            private CopyBroadcastReceiver _toClientBuffer;
            private IRingBuffer _toDriverBuffer;
            private ILogBuffersFactory _logBuffersFactory;
            private ErrorHandler _errorHandler;
            private AvailableImageHandler _availableImageHandler;
            private UnavailableImageHandler _unavailableImageHandler;
            private long _keepAliveInterval = KeepaliveIntervalNs;
            private long _interServiceTimeout = InterServiceTimeoutNs;
            private long _publicationConnectionTimeout = PublicationConnectionTimeoutMs;
            private FileInfo _cncFile;
            private string _aeronDirectoryName;
            private long _driverTimeoutMs = DEFAULT_DRIVER_TIMEOUT_MS;
            private MappedByteBuffer _cncByteBuffer;
            private UnsafeBuffer _cncMetaDataBuffer;
            private UnsafeBuffer _countersMetaDataBuffer;
            private UnsafeBuffer _countersValuesBuffer;
            private MapMode _imageMapMode = MapMode.ReadOnly;
            private IThreadFactory threadFactory = new DefaultThreadFactory();

            /// <summary>
            /// The top level Aeron directory used for communication between a Media Driver and client.
            /// </summary>
            public const string AERON_DIR_PROP_NAME = "aeron.dir";

            /// <summary>
            /// The value of the top level Aeron directory unless overridden by <seealso cref="AeronDirectoryName()"/>
            /// </summary>
            public static readonly string AERON_DIR_PROP_DEFAULT = Path.Combine(IoUtil.TmpDirName(), "aeron-" + Environment.UserName);

            /// <summary>
            /// URI used for IPC <seealso cref="Publication"/>s and  <seealso cref="Subscription"/>s
            /// </summary>
            public const string IPC_CHANNEL = "aeron:ipc";

            /// <summary>
            /// Timeout in which the driver is expected to respond.
            /// </summary>
            public const long DEFAULT_DRIVER_TIMEOUT_MS = 10000;

            /// <summary>
            /// The param name to be used for the term length as a channel URI param.
            /// </summary>
            public const string TERM_LENGTH_PARAM_NAME = "term-length";

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

            public Context()
            {
                _aeronDirectoryName = Config.GetProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
            }

            /// <summary>
            /// This is called automatically by <seealso cref="Connect()"/> and its overloads.
            /// There is no need to call it from a client application. It is responsible for providing default
            /// values for options that are not individually changed through field setters.
            /// </summary>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context Conclude()
            {
                _cncFile = new FileInfo(Path.Combine(_aeronDirectoryName, CncFileDescriptor.CNC_FILE));

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

                if (_toClientBuffer == null)
                {
                    var receiver =
                        new BroadcastReceiver(CncFileDescriptor.CreateToClientsBuffer(_cncByteBuffer,
                            _cncMetaDataBuffer));
                    _toClientBuffer = new CopyBroadcastReceiver(receiver);
                }

                if (_toDriverBuffer == null)
                {
                    _toDriverBuffer =
                        new ManyToOneRingBuffer(CncFileDescriptor.CreateToDriverBuffer(_cncByteBuffer,
                            _cncMetaDataBuffer));
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

                return this;
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
            /// This method is used for testing and debugging.
            /// </summary>
            /// <param name="logBuffersFactory"> Injected LogBuffersFactory </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context BufferManager(ILogBuffersFactory logBuffersFactory)
            {
                _logBuffersFactory = logBuffersFactory;
                return this;
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
            /// Set up a callback for when an <seealso cref="Image"/> is available.
            /// </summary>
            /// <param name="handler"> Callback method for handling available image notifications. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context AvailableImageHandler(AvailableImageHandler handler)
            {
                _availableImageHandler = handler;
                return this;
            }

            /// <summary>
            /// Set up a callback for when an <seealso cref="Image"/> is unavailable.
            /// </summary>
            /// <param name="handler"> Callback method for handling unavailable image notifications. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            public Context UnavailableImageHandler(UnavailableImageHandler handler)
            {
                _unavailableImageHandler = handler;
                return this;
            }


            /// <summary>
            /// Get the buffer containing the counter meta data.
            /// </summary>
            /// <returns> The buffer storing the counter meta data. </returns>
            public UnsafeBuffer CountersMetaDataBuffer()
            {
                return _countersMetaDataBuffer;
            }

            /// <summary>
            /// Set the buffer containing the counter meta data.
            /// </summary>
            /// <param name="countersMetaDataBuffer"> The new counter meta data buffer. </param>
            /// <returns> this Object for method chaining. </returns>
            public Context CountersMetaDataBuffer(UnsafeBuffer countersMetaDataBuffer)
            {
                _countersMetaDataBuffer = countersMetaDataBuffer;
                return this;
            }


            /// <summary>
            /// Get the buffer containing the counters.
            /// </summary>
            /// <returns> The buffer storing the counters. </returns>
            public UnsafeBuffer CountersValuesBuffer()
            {
                return _countersValuesBuffer;
            }

            /// <summary>
            /// Set the buffer containing the counters
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
            /// <seealso cref="ErrorHandler" />
            public Context DriverTimeoutMs(long driverTimeoutMs)
            {
                _driverTimeoutMs = driverTimeoutMs;
                return this;
            }

            /// <summary>
            /// Return the timeout between service calls for the client.
            /// 
            /// When exceeded, <seealso cref="ErrorHandler"/> will be called and the active <seealso cref="Publication"/>s and <seealso cref="Image"/>s
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
            /// Specify the thread factory to use when starting the conductor thread.
            /// </summary>
            /// <param name="threadFactory"> thread factory to construct the thread.</param>
            /// <returns> this for a fluent API.</returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                this.threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// The thread factory to be use to construct the conductor thread
            /// </summary>
            /// <returns>the specified thread factory of <see cref="DefaultThreadFactory"/> if none is provided.</returns>
            public IThreadFactory ThreadFactory()
            {
                return threadFactory;
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
                _cncMetaDataBuffer?.Dispose();
                _countersMetaDataBuffer?.Dispose();
                _countersValuesBuffer?.Dispose();
                _cncByteBuffer?.Dispose();
            }
            
            internal ClientConductor CreateClientConductor()
            {
                return new ClientConductor(_epochClock, _nanoClock, _toClientBuffer, _logBuffersFactory,
                    _countersValuesBuffer, new DriverProxy(_toDriverBuffer), _errorHandler,
                    _availableImageHandler, _unavailableImageHandler, _imageMapMode, _keepAliveInterval, _driverTimeoutMs,
                    _interServiceTimeout, _publicationConnectionTimeout);
            }

            internal AgentRunner CreateConductorRunner(ClientConductor clientConductor)
            {
                return new AgentRunner(_idleStrategy, _errorHandler, null, clientConductor);
            }

            private void ConnectToDriver()
            {
                var startMs = _epochClock.Time();

                while (!_cncFile.Exists)
                {
                    if (_epochClock.Time() > startMs + _driverTimeoutMs)
                    {
                        throw new DriverTimeoutException("CnC file is created by not initialised.");
                    }

                    LockSupport.ParkNanos(1);
                    //TODO was Thread.Yield();
                }
                
                _cncByteBuffer = IoUtil.MapExistingFile(CncFile().FullName, MapMode.ReadWrite);
                _cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(_cncByteBuffer);
                
                int cncVersion;
                while (0 == (cncVersion = _cncMetaDataBuffer.GetInt(CncFileDescriptor.CncVersionOffset(0))))
                {
                    if (_epochClock.Time() > startMs + _driverTimeoutMs)
                    {
                        throw new DriverTimeoutException("CnC file is created by not initialised.");
                    }

                    LockSupport.ParkNanos(1);
                    //TODO was Thread.Yield();
                }

                if (CncFileDescriptor.CNC_VERSION != cncVersion)
                {
                    throw new InvalidOperationException(
                        "aeron cnc file version not understood: version=" + cncVersion);
                }
            }
        }
    }
}
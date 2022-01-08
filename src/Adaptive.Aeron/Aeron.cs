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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
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
    /// occurs then the process will face the wrath of <seealso cref="Environment.Exit"/>. See <seealso cref="Configuration.DEFAULT_ERROR_HANDLER"/>.
    /// 
    /// </summary>
    public class Aeron : IDisposable
    {
        /// <summary>
        /// Used to represent a null value for when some value is not yet set.
        /// </summary>
        public const int NULL_VALUE = -1;

        private readonly AtomicBoolean _isClosed = new AtomicBoolean(false);
        private readonly long _clientId;
        private readonly ClientConductor _conductor;
        private readonly IRingBuffer _commandBuffer;
        private readonly AgentInvoker _conductorInvoker;
        private readonly AgentRunner _conductorRunner;
        private readonly Context _ctx;

        internal Aeron(Context ctx)
        {
            try
            {
                ctx.Conclude();

                _ctx = ctx;
                _clientId = ctx.ClientId();
                _commandBuffer = ctx.ToDriverBuffer();
                _conductor = new ClientConductor(ctx, this);

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
            catch (ConcurrentConcludeException)
            {
                throw;
            }
            catch (Exception)
            {
                CloseHelper.QuietDispose(ctx);
                throw;
            }
        }

        /// <summary>
        /// Create an Aeron instance and connect to the media driver with a default <seealso cref="Context"/>.
        /// 
        /// Threads required for interacting with the media driver are created and managed within the Aeron instance.
        /// 
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
                    aeron.ConductorAgentInvoker.Start();
                }
                else
                {
                    AgentRunner.StartOnThread(aeron._conductorRunner, ctx.ThreadFactory());
                }

                return aeron;
            }
            catch (ConcurrentConcludeException)
            {
                throw;
            }
            catch (Exception)
            {
                ctx.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Print out the values from <seealso cref="CountersReader"/> which can be useful for debugging.
        /// </summary>
        ///  <param name="out"> to where the counters get printed. </param>
        public void PrintCounters(StreamWriter @out)
        {
            CountersReader counters = CountersReader;
            counters.ForEach((value, id, label) => @out.WriteLine("{0,3}: {1:} - {2}", id, value, label));
        }


        /// <summary>
        /// Has the client been closed? If not then the CnC file may not be unmapped.
        /// </summary>
        /// <returns> true if the client has been explicitly closed otherwise false. </returns>
        public bool IsClosed => _isClosed.Get();

        /// <summary>
        /// Get the <seealso cref="Context"/> that is used by this client.
        /// </summary>
        /// <returns> the <seealso cref="Context"/> that is use by this client. </returns>
        public Context Ctx => _ctx;

        /// <summary>
        /// Get the client identity that has been allocated for communicating with the media driver.
        /// </summary>
        /// <returns> the client identity that has been allocated for communicating with the media driver. </returns>
        public long ClientId => _clientId;

        /// <summary>
        /// Get the <seealso cref="AgentInvoker"/> for the client conductor.
        /// </summary>
        /// <returns> the <seealso cref="AgentInvoker"/> for the client conductor. </returns>
        public AgentInvoker ConductorAgentInvoker => _conductorInvoker;

        /// <summary>
        /// Is the command still active for a given correlation id.
        /// </summary>
        /// <param name="correlationId"> to check if it is still active. </param>
        /// <returns> true in the command is still in active processing or false if completed successfully or errored. </returns>
        /// <seealso cref="Publication.AsyncAddDestination(String)"></seealso>
        /// <seealso cref="Subscription.AsyncAddDestination(String)"></seealso>
        /// <seealso cref="HasActiveCommands"/>
        public bool IsCommandActive(long correlationId)
        {
            return _conductor.IsCommandActive(correlationId);
        }

        /// <summary>
        /// Does the client have any active asynchronous commands?
        /// <para>
        /// When close operations are performed on <seealso cref="Publication"/>s, <seealso cref="Subscription"/>s, and <seealso cref="Counter"/>s the
        /// commands are sent asynchronously to the driver. The client tracks active commands in case errors need to be
        /// reported. If you wish to wait for acknowledgement of close operations then wait for this method to return false.
        /// 
        /// </para>
        /// </summary>
        /// <returns> true if any commands are currently active otherwise false. </returns>
        public bool HasActiveCommands()
        {
            return _conductor.HasActiveCommands();
        }

        /// <summary>
        /// Clean up and release all Aeron client resources and shutdown conducator thread if not using
        /// <see cref="Context.UseConductorAgentInvoker(bool)"/>.
        /// 
        /// This will close all currently open <see cref="Publication"/>s, <see cref="Subscription"/>s and <see cref="Counter"/>s created
        /// from this client. To check for the command being acknowledged by the driver.
        /// </summary>
        public void Dispose()
        {
            if (_isClosed.CompareAndSet(false, true))
            {
                ErrorHandler errorHandler = _ctx.ErrorHandler();
                if (null != _conductorRunner)
                {
                    CloseHelper.Dispose(errorHandler, _conductorRunner);
                }
                else
                {
                    CloseHelper.Dispose(errorHandler, _conductorInvoker);
                }
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
        /// Asynchronously add a <seealso cref="Publication"/> for publishing messages to subscribers. The added publication returned
        /// is threadsafe.
        /// </summary>
        /// <param name="channel">  for sending the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the registration id of the publication which can be used to get the added publication. </returns>
        /// <seealso cref="GetPublication"/>
        public long AsyncAddPublication(string channel, int streamId)
        {
            return _conductor.AsyncAddPublication(channel, streamId);
        }

        /// <summary>
        /// Asynchronously add a <seealso cref="Publication"/> for publishing messages to subscribers from a single thread.
        /// </summary>
        /// <param name="channel">  for sending the messages known to the media layer. </param>
        /// <param name="streamId"> within the channel scope. </param>
        /// <returns> the registration id of the publication which can be used to get the added exclusive publication. </returns>
        /// <seealso cref="GetExclusivePublication"/>
        public long AsyncAddExclusivePublication(string channel, int streamId)
        {
            return _conductor.AsyncAddExclusivePublication(channel, streamId);
        }

        /// <summary>
        /// Get a <seealso cref="Publication"/> for publishing messages to subscribers. The publication returned is threadsafe.
        /// </summary>
        /// <param name="registrationId"> returned from <seealso cref="AsyncAddPublication"/>. </param>
        /// <returns> a new <seealso cref="ConcurrentPublication"/> when available otherwise null. </returns>
        /// <seealso cref="AsyncAddPublication"/>
        public ConcurrentPublication GetPublication(long registrationId)
        {
            return _conductor.GetPublication(registrationId);
        }

        /// <summary>
        /// Get a single threaded <seealso cref="Publication"/> for publishing messages to subscribers.
        /// </summary>
        /// <param name="registrationId"> returned from <seealso cref="AsyncAddExclusivePublication"/>. </param>
        /// <returns> a new <seealso cref="ExclusivePublication"/> when available otherwise null. </returns>
        /// <seealso cref="AsyncAddExclusivePublication"/>
        public ExclusivePublication GetExclusivePublication(long registrationId)
        {
            return _conductor.GetExclusivePublication(registrationId);
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
                throw new AeronException("client is closed");
            }

            return _commandBuffer.NextCorrelationId();
        }

        /// <summary>
        /// Get the <see cref="CountersReader"/> for the Aeron media driver counters.
        /// </summary>
        /// <returns> new <see cref="CountersReader"/> for the Aeron media driver in use.</returns>
        public CountersReader CountersReader
        {
            get
            {
                if (_conductor.IsClosed())
                {
                    throw new AeronException("client is closed");
                }

                return _conductor.CountersReader();
            }
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
        /// The counter should be freed by calling <seealso cref="Counter.Dispose()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="typeId"> for the counter. </param>
        /// <param name="label">  for the counter. It should be US-ASCII. </param>
        /// <returns> the newly allocated counter. </returns>
        /// <seealso cref="CountersManager.Allocate(string,int)"></seealso>
        public Counter AddCounter(int typeId, string label)
        {
            return _conductor.AddCounter(typeId, label);
        }

        /// <summary>
        /// Add a handler to the list be called when <seealso cref="Counter"/>s become available.
        /// </summary>
        /// <param name="handler"> to be called when <seealso cref="Counter"/>s become available. </param>
        /// <returns> registration id for the handler which can be used to remove it. </returns>
        public long AddAvailableCounterHandler(AvailableCounterHandler handler)
        {
            return _conductor.AddAvailableCounterHandler(handler);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when <seealso cref="Counter"/>s become available.
        /// </summary>
        /// <param name="registrationId"> to be removed which was returned from add method. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        public bool RemoveAvailableCounterHandler(long registrationId)
        {
            return _conductor.RemoveAvailableCounterHandler(registrationId);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when <seealso cref="Counter"/>s become available.
        /// </summary>
        /// <param name="handler"> to be removed. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        [Obsolete]
        public bool RemoveAvailableCounterHandler(AvailableCounterHandler handler)
        {
            return _conductor.RemoveAvailableCounterHandler(handler);
        }

        /// <summary>
        /// Add a handler to the list be called when <seealso cref="Counter"/>s become unavailable.
        /// </summary>
        /// <param name="handler"> to be called when <seealso cref="Counter"/>s become unavailable. </param>
        /// <returns> registration id for the handler which can be used to remove it. </returns>
        public long AddUnavailableCounterHandler(UnavailableCounterHandler handler)
        {
            return _conductor.AddUnavailableCounterHandler(handler);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when <seealso cref="Counter"/>s become unavailable.
        /// </summary>
        /// <param name="registrationId"> to be removed which was returned from add method. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        public bool RemoveUnavailableCounterHandler(long registrationId)
        {
            return _conductor.RemoveUnavailableCounterHandler(registrationId);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when <seealso cref="Counter"/>s become unavailable.
        /// </summary>
        /// <param name="handler"> to be removed. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        [Obsolete]
        public bool RemoveUnavailableCounterHandler(UnavailableCounterHandler handler)
        {
            return _conductor.RemoveUnavailableCounterHandler(handler);
        }

        /// <summary>
        /// Add a handler to the list be called when the Aeron client is closed.
        /// </summary>
        /// <param name="handler"> to be called when the Aeron client is closed. </param>
        /// <returns> registration id for the handler which can be used to remove it. </returns>
        public long AddCloseHandler(Action handler)
        {
            return _conductor.AddCloseHandler(handler);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when the Aeron client is closed.
        /// </summary>
        /// <param name="handler"> to be removed. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        [Obsolete]
        public bool RemoveCloseHandler(Action handler)
        {
            return _conductor.RemoveCloseHandler(handler);
        }

        /// <summary>
        /// Remove a previously added handler to the list be called when the Aeron client is closed.
        /// </summary>
        /// <param name="registrationId"> of the handler from when it was added. </param>
        /// <returns> true if found and removed otherwise false. </returns>
        public bool RemoveCloseHandler(long registrationId)
        {
            return _conductor.RemoveCloseHandler(registrationId);
        }

        /// <summary>
        /// Called by the <seealso cref="ClientConductor"/> if the client should be terminated due to timeout.
        /// </summary>
        internal void InternalClose()
        {
            _isClosed.Set(true);
        }

        public static class Configuration
        {
            /*
             * Duration in milliseconds for which the client conductor will sleep between duty cycles.
             */
            public static readonly int IdleSleepMs = 16;

            /// <summary>
            /// Duration in milliseconds for which the client will sleep when awaiting a response from the driver.
            /// </summary>
            public static readonly int AWAITING_IDLE_SLEEP_MS = 1;

            /*
            * Duration in nanoseconds for which the client conductor will sleep between duty cycles.
            */
            public static readonly long IdleSleepNs = NanoUtil.FromMilliseconds(IdleSleepMs);

            /*
             * Default interval between sending keepalive control messages to the driver.
             */
            public static readonly long KeepaliveIntervalNs = NanoUtil.FromMilliseconds(500);

            /// <summary>
            /// Duration to wait while lingering an entity such as an <seealso cref="Image"/> before deleting underlying resources
            /// such as memory mapped files.
            /// </summary>
            public const string RESOURCE_LINGER_DURATION_PROP_NAME = "aeron.client.resource.linger.duration";

            /// <summary>
            /// Default duration a resource should linger before deletion.
            /// </summary>
            public static readonly long RESOURCE_LINGER_DURATION_DEFAULT_NS = NanoUtil.FromSeconds(3);

            /// <summary>
            /// Duration to linger on close so that publishers subscribers have time to notice closed resources.
            /// This value can be set to a few seconds if the application is likely to experience CPU starvation or
            /// long GC pauses.
            /// </summary>
            public const string CLOSE_LINGER_DURATION_PROP_NAME = "aeron.client.close.linger.duration";

            /// <summary>
            /// Default duration to linger on close so that publishers subscribers have time to notice closed resources.
            /// </summary>
            public const long CLOSE_LINGER_DURATION_DEFAULT_NS = 0;

            /// <summary>
            /// Should memory-mapped files be pre-touched so that they are already faulted into a process.
            /// <para>
            /// Pre-touching files can result in it taking taking longer for resources to become available in
            /// return for avoiding later pauses due to page faults.
            /// </para>
            /// </summary>
            public const string PRE_TOUCH_MAPPED_MEMORY_PROP_NAME = "aeron.pre.touch.mapped.memory";

            /// <summary>
            /// Default for if a memory-mapped filed should be pre-touched to fault it into a process.
            /// </summary>
            public const bool PRE_TOUCH_MAPPED_MEMORY_DEFAULT = false;

            /// <summary>
            /// The Default handler for Aeron runtime exceptions.
            /// When a <seealso cref="DriverTimeoutException"/> is encountered, this handler will exit the program.
            /// <para>
            /// The error handler can be overridden by supplying an <seealso cref="Context"/> with a custom handler.
            /// 
            /// </para>
            /// </summary>
            /// <seealso cref="Context.ErrorHandler(ErrorHandler)" />
            public static readonly ErrorHandler DEFAULT_ERROR_HANDLER = (throwable) =>
            {
                lock (Console.Error)
                {
                    Console.Error.WriteLine(throwable);
                }

                if (throwable is DriverTimeoutException)
                {
                    Console.Error.WriteLine();
                    Console.Error.WriteLine("***");
                    Console.Error.WriteLine("*** Timeout for the Media Driver - is it currently running? exiting");
                    Console.Error.WriteLine("***");
                    Environment.Exit(-1);
                }
            };

            /// <summary>
            /// Duration to wait while lingering an entity such as an <seealso cref="Image"/> before deleting underlying resources
            /// such as memory mapped files.
            /// </summary>
            /// <returns> duration in nanoseconds to wait before deleting an expired resource. </returns>
            /// <seealso cref="RESOURCE_LINGER_DURATION_PROP_NAME"/>
            public static long ResourceLingerDurationNs()
            {
                return Config.GetDurationInNanos(RESOURCE_LINGER_DURATION_PROP_NAME,
                    RESOURCE_LINGER_DURATION_DEFAULT_NS);
            }

            /// <summary>
            /// Duration to wait while lingering an entity such as an <seealso cref="Image"/> before deleting underlying resources
            /// such as memory mapped files.
            /// </summary>
            /// <returns> duration in nanoseconds to wait before deleting an expired resource. </returns>
            /// <seealso cref="RESOURCE_LINGER_DURATION_PROP_NAME"/>
            public static long CloseLingerDurationNs()
            {
                return Config.GetDurationInNanos(CLOSE_LINGER_DURATION_PROP_NAME, CLOSE_LINGER_DURATION_DEFAULT_NS);
            }

            /// <summary>
            /// Should memory-mapped files be pre-touched so that they are already faulted into a process.
            /// </summary>
            /// <returns> true if memory mappings should be pre-touched, otherwise false. </returns>
            /// <seealso cref="PRE_TOUCH_MAPPED_MEMORY_PROP_NAME"/>
            public static bool PreTouchMappedMemory()
            {
                string value = Config.GetProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME);
                if (null != value)
                {
                    return bool.Parse(value);
                }

                return PRE_TOUCH_MAPPED_MEMORY_DEFAULT;
            }
        }

        /// <summary>
        /// Provides a means to override configuration for an <seealso cref="Aeron"/> class via the <seealso cref="Aeron.Connect(Aeron.Context)"/>
        /// method and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
        /// It can also set up error handling as well as application callbacks for image information from the Media Driver.
        /// 
        /// A number of the properties are for testing and should not be set by end users.
        /// 
        /// <b>Note:</b> Do not reuse instances of the context across different <seealso cref="Aeron"/> clients.
        ///
        /// The context will be owned be <see cref="ClientConductor"/> after a successful
        /// <see cref="Aeron.Connect(Context)"/> and closed via <see cref="Aeron.Dispose"/>
        /// </summary>
        public class Context : IDisposable
        {
            private long _clientId;
            private bool _useConductorAgentInvoker = false;
            private bool _preTouchMappedMemory = Configuration.PreTouchMappedMemory();
            private AgentInvoker _driverAgentInvoker;
            private ILock _clientLock;
            private IEpochClock _epochClock;
            private INanoClock _nanoClock;
            private IIdleStrategy _idleStrategy;
            private IIdleStrategy _awaitingIdleStrategy;
            private CopyBroadcastReceiver _toClientBuffer;
            private IRingBuffer _toDriverBuffer;
            private DriverProxy _driverProxy;
            private ILogBuffersFactory _logBuffersFactory;
            private ErrorHandler _errorHandler;
            private ErrorHandler _subscriberErrorHandler;
            private AvailableImageHandler _availableImageHandler;
            private UnavailableImageHandler _unavailableImageHandler;
            private AvailableCounterHandler _availableCounterHandler;
            private UnavailableCounterHandler _unavailableCounterHandler;
            private Action _closeHandler;
            private long _keepAliveIntervalNs = Configuration.KeepaliveIntervalNs;
            private long _interServiceTimeoutNs = 0;
            private long _resourceLingerDurationNs = Configuration.ResourceLingerDurationNs();
            private long _closeLingerDurationNs = Configuration.CloseLingerDurationNs();
            private FileInfo _cncFile;
            private string _aeronDirectoryName = GetAeronDirectoryName();
            private DirectoryInfo _aeronDirectory;
            private long _driverTimeoutMs = DRIVER_TIMEOUT_MS;
            private MappedByteBuffer _cncByteBuffer;
            private UnsafeBuffer _cncMetaDataBuffer;
            private UnsafeBuffer _countersMetaDataBuffer;
            private UnsafeBuffer _countersValuesBuffer;
            private IThreadFactory _threadFactory = new DefaultThreadFactory();
            private int _isConcluded = 0;

            static Context()
            {
                string baseDirName = null;

                if (Environment.OSVersion.Platform == PlatformID.Unix)
                {
                    if (Directory.Exists(@"/dev/shm"))
                    {
                        baseDirName = "/dev/shm/aeron";
                    }
                }

                if (null == baseDirName)
                {
                    baseDirName = Path.Combine(Path.GetTempPath(), "aeron");
                }

                AERON_DIR_PROP_DEFAULT = baseDirName + "-" + Environment.UserName;
            }

            /// <summary>
            /// The top level Aeron directory used for communication between a Media Driver and client.
            /// </summary>
            public const string AERON_DIR_PROP_NAME = "aeron.dir";

            /// <summary>
            /// The value of the top level Aeron directory unless overridden by <seealso cref="AeronDirectoryName()"/>
            /// </summary>
            public static readonly string AERON_DIR_PROP_DEFAULT;

            /// <summary>
            /// Media type used for IPC shared memory from <seealso cref="Publication"/> to <seealso cref="Subscription"/> channels.
            /// </summary>
            public const string IPC_MEDIA = "ipc";

            /// <summary>
            /// Media type used for UDP sockets from <seealso cref="Publication"/> to <seealso cref="Subscription"/> channels.
            /// </summary>
            public const string UDP_MEDIA = "udp";

            /// <summary>
            /// URI base used for IPC channels for <seealso cref="Publication"/>s and <seealso cref="Subscription"/>s
            /// </summary>
            public const string IPC_CHANNEL = "aeron:ipc";

            /// <summary>
            /// URI base used for UDP channels for <seealso cref="Publication"/>s and <seealso cref="Subscription"/>s
            /// </summary>
            public const string UDP_CHANNEL = "aeron:udp";

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
            /// Property name for the timeout to use in debug mode. By default, this is not set and the configured
            /// timeouts will be used. Setting this value adjusts timeouts to make debugging easier.
            /// </summary>
            public const string DEBUG_TIMEOUT_PROP_NAME = "aeron.debug.timeout";

            /// <summary>
            /// Timeout in which the driver is expected to respond.
            /// </summary>
            public const long DRIVER_TIMEOUT_MS = 10000;

            /// <summary>
            /// Value to represent a sessionId that is not to be used.
            /// </summary>
            public const int NULL_SESSION_ID = Aeron.NULL_VALUE;

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
            /// Valid value for <seealso cref="MDC_CONTROL_MODE"/> when manual control is desired.
            /// </summary>
            public const string MDC_CONTROL_MODE_MANUAL = "manual";

            /// <summary>
            /// Valid value for <seealso cref="MDC_CONTROL_MODE_PARAM_NAME"/> when dynamic control is desired. Default value.
            /// </summary>
            public const string MDC_CONTROL_MODE_DYNAMIC = "dynamic";

            /// <summary>
            /// Key for the session id for a publication or restricted subscription.
            /// </summary>
            public const string SESSION_ID_PARAM_NAME = "session-id";

            /// <summary>
            /// Key for timeout a publication to linger after draining in nanoseconds.
            /// </summary>
            public const string LINGER_PARAM_NAME = "linger";

            /// <summary>
            /// Parameter name for channel URI param to indicate if a subscribed stream must be reliable or not.
            /// Value is boolean with true to recover loss and false to gap fill.
            /// </summary>
            public const string RELIABLE_STREAM_PARAM_NAME = "reliable";

            /// <summary>
            /// Key for the tags for a channel
            /// </summary>
            public const string TAGS_PARAM_NAME = "tags";

            /// <summary>
            /// Qualifier for a value which is a tag for reference. This prefix is use in the param value.
            /// </summary>
            public const string TAG_PREFIX = "tag:";

            /// <summary>
            /// Parameter name for channel URI param to indicate if term buffers should be sparse. Value is boolean.
            /// </summary>
            public const string SPARSE_PARAM_NAME = "sparse";

            /// <summary>
            /// Parameter name for channel URI param to indicate an alias for the given URI. Value not interpreted by Aeron.
            ///
            /// This is a reserved application level param used to identify a particular channel for application purposes.
            /// </summary>
            public const string ALIAS_PARAM_NAME = "alias";

            /// <summary>
            /// Parameter name for channel URI param to indicate if End of Stream (EOS) should be sent or not. Value is boolean.
            /// </summary>
            public const string EOS_PARAM_NAME = "eos";

            /// <summary>
            /// Parameter name for channel URI param to indicate if a subscription should tether for local flow control.
            /// Value is boolean. A tether only applies when there is more than one matching active subscription. If tether is
            /// true then that subscription is included in flow control. If only one subscription then it tethers pace.
            /// </summary>
            public const string TETHER_PARAM_NAME = "tether";

            /// <summary>
            /// Parameter name for channel URI param to indicate if a Subscription represents a group member or individual
            /// from the perspective of message reception. This can inform loss handling and similar semantics.
            /// <para>
            /// When configuring a subscription for an MDC publication then should be added as this is effective multicast.
            ///    
            /// </para>
            /// </summary>
            /// <seealso cref="MDC_CONTROL_MODE_PARAM_NAME"></seealso>
            /// <seealso cref="MDC_CONTROL_PARAM_NAME"></seealso>
            public const string GROUP_PARAM_NAME = "group";

            /// <summary>
            /// Parameter name for Subscription URI param to indicate if Images that go unavailable should be allowed to
            /// rejoin after a short cooldown or not.
            /// </summary>
            public const string REJOIN_PARAM_NAME = "rejoin";

            /// <summary>
            /// Parameter name for Subscription URI param to indicate the congestion control algorithm to be used.
            /// Options include {@code static} and {@code cubic}.
            /// </summary>
            public const string CONGESTION_CONTROL_PARAM_NAME = "cc";

            /// <summary>
            /// Parameter name for Publication URI param to indicate the flow control strategy to be used.
            /// Options include {@code min}, {@code max}, and {@code pref}.
            /// </summary>
            public const string FLOW_CONTROL_PARAM_NAME = "fc";

            /// <summary>
            /// Parameter name for Subscription URI param to indicate the receiver tag to be sent in SMs.
            /// </summary>
            public const string GROUP_TAG_PARAM_NAME = "gtag";

            /// <summary>
            /// Parameter name for Publication URI param to indicate whether spy subscriptions should simulate a connection.
            /// </summary>
            public const string SPIES_SIMULATE_CONNECTION_PARAM_NAME = "ssc";

            /// <summary>
            /// Parameter name for the underlying OS socket send buffer length.
            /// </summary>
            public const string SOCKET_SNDBUF_PARAM_NAME = "so-sndbuf";

            /// <summary>
            /// Parameter name for the underlying OS socket receive buffer length.
            /// </summary>
            public const string SOCKET_RCVBUF_PARAM_NAME = "so-rcvbuf";

            /// <summary>
            /// Parameter name for the congestion control's initial receiver window length.
            /// </summary>
            public const string RECEIVER_WINDOW_LENGTH_PARAM_NAME = "rcv-wnd";

            /// <summary>
            /// Parameter name of the offset for the media receive timestamp to be inserted into the incoming message on a
            /// subscription. The special value of 'reserved' can be used to insert into the reserved value field. Media
            /// receive timestamp is taken as the earliest possible point after the packet is received from the network. This
            /// is only supported in the C media driver, the Java Media Driver will generate an error if used.
            /// </summary>
            public const string MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME = "media-rcv-ts-offset";

            /// <summary>
            /// Parameter name of the offset for the channel receive timestamp to be inserted into the incoming message on a
            /// subscription. The special value of 'reserved' can be used to insert into the reserved value field. Channel
            /// receive timestamp is taken as soon a possible after the packet is received by Aeron receiver from the transport
            /// bindings.
            /// </summary>
            public const string CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME = "channel-rcv-ts-offset";

            /// <summary>
            /// Parameter name of the offset for the channel send timestamp to be inserted into the outgoing message
            /// on a publication. The special value of 'reserved' can be used to insert into the reserved value
            /// field. Channel send timestamp is taken shortly before passing the message over to the configured transport
            /// bindings.
            /// </summary>
            public const string CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME = "channel-snd-ts-offset";

            /// <summary>
            /// Placeholder value to use in URIs to specify that a timestamp should be stored in the reserved value field.
            /// </summary>
            public const string RESERVED_OFFSET = "reserved";

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
            /// Has the context had the <seealso cref="Conclude()"/> method called.
            /// </summary>
            /// <returns> true of the <seealso cref="Conclude()"/> method has been called. </returns>
            public bool IsConcluded => 1 == _isConcluded;

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
            public Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// This is called automatically by <seealso cref="Connect()"/> and its overloads.
            /// There is no need to call it from a client application. It is responsible for providing default
            /// values for options that are not individually changed through field setters.
            /// </summary>
            /// <returns> this for a fluent API. </returns>
            public Context Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

                ConcludeAeronDirectory();

                _cncFile = new FileInfo(Path.Combine(_aeronDirectory.FullName, CncFileDescriptor.CNC_FILE));

                if (null == _clientLock)
                {
                    _clientLock = new ReentrantLock();
                }

                if (_epochClock == null)
                {
                    _epochClock = SystemEpochClock.INSTANCE;
                }

                if (_nanoClock == null)
                {
                    _nanoClock = SystemNanoClock.INSTANCE;
                }

                if (_idleStrategy == null)
                {
                    _idleStrategy = new SleepingIdleStrategy(Configuration.IdleSleepMs);
                }

                if (null == _awaitingIdleStrategy)
                {
                    _awaitingIdleStrategy = new SleepingIdleStrategy(Configuration.AWAITING_IDLE_SLEEP_MS);
                }

                if (CncFile() != null)
                {
                    ConnectToDriver();
                }

                _interServiceTimeoutNs = CncFileDescriptor.ClientLivenessTimeoutNs(_cncMetaDataBuffer);
                if (_interServiceTimeoutNs <= _keepAliveIntervalNs)
                {
                    throw new ConfigurationException("interServiceTimeoutNs=" + _interServiceTimeoutNs +
                                                     " <= keepAliveIntervalNs=" + _keepAliveIntervalNs);
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

                if (_logBuffersFactory == null)
                {
                    _logBuffersFactory = new MappedLogBuffersFactory();
                }

                if (_errorHandler == null)
                {
                    _errorHandler = Configuration.DEFAULT_ERROR_HANDLER;
                }

                if (_subscriberErrorHandler == null)
                {
                    _subscriberErrorHandler = _errorHandler;
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
            /// <returns> this for a fluent API. </returns>
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
            /// Should mapped-memory be pre-touched to avoid soft page faults.
            /// </summary>
            /// <param name="preTouchMappedMemory"> true if mapped-memory should be pre-touched otherwise false. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.PRE_TOUCH_MAPPED_MEMORY_PROP_NAME"/>
            public Context PreTouchMappedMemory(bool preTouchMappedMemory)
            {
                _preTouchMappedMemory = preTouchMappedMemory;
                return this;
            }

            /// <summary>
            /// Should mapped-memory be pre-touched to avoid soft page faults.
            /// </summary>
            /// <returns> true if mapped-memory should be pre-touched otherwise false. </returns>
            /// <seealso cref="Configuration.PRE_TOUCH_MAPPED_MEMORY_PROP_NAME"/>
            public bool PreTouchMappedMemory()
            {
                return _preTouchMappedMemory;
            }

            /// <summary>
            /// Set the <seealso cref="AgentInvoker"/> for the Media Driver to be used while awaiting a synchronous response.
            /// <para>
            /// Useful for when running on a low thread count scenario.
            /// 
            /// </para>
            /// </summary>
            /// <param name="driverAgentInvoker"> to be invoked while awaiting a response in the client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context DriverAgentInvoker(AgentInvoker driverAgentInvoker)
            {
                _driverAgentInvoker = driverAgentInvoker;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="AgentInvoker"/> that is used to run the Media Driver while awaiting a synchronous response.
            /// </summary>
            /// <returns> the <seealso cref="AgentInvoker"/> that is used for running the Media Driver. </returns>
            public AgentInvoker DriverAgentInvoker()
            {
                return _driverAgentInvoker;
            }

            /// <summary>
            /// The <see cref="ILock"/> that is used to provide mutual exclusion in the Aeron client.
            /// 
            /// If the <see cref="UseConductorAgentInvoker(bool)"/> is set and only one thread accesses the client
            /// then the lock can be set to <see cref="NoOpLock"/> to elide the lock overhead.
            /// 
            /// </summary>
            /// <param name="lock"> that is used to provide mutual exclusion in the Aeron client.</param>
            /// <returns> this for a fluent API. </returns>
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
            /// <returns>this for a fluent API.</returns>
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
            /// Set the <seealso cref="INanoClock"/> to be used for tracking high resolution time.
            /// </summary>
            /// <param name="clock"> <seealso cref="INanoClock"/> to be used for tracking high resolution time. </param>
            /// <returns>this for a fluent API.</returns>
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
            /// Provides an <seealso cref="IIdleStrategy"/> for the thread responsible for the client duty cycle.
            /// </summary>
            /// <param name="idleStrategy"> Thread idle strategy for the client duty cycle. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategy(IIdleStrategy idleStrategy)
            {
                _idleStrategy = idleStrategy;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IIdleStrategy"/> employed by the client for the client duty cycle.
            /// </summary>
            /// <returns> the <seealso cref="IIdleStrategy"/> employed by the client for the client duty cycle. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return _idleStrategy;
            }

            /// <summary>
            /// Provides an <seealso cref="IIdleStrategy"/> to be used when awaiting a response from the Media Driver.
            /// </summary>
            /// <param name="idleStrategy"> Thread idle strategy for awaiting a response from the Media Driver. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AwaitingIdleStrategy(IIdleStrategy idleStrategy)
            {
                _awaitingIdleStrategy = idleStrategy;
                return this;
            }

            /// <summary>
            /// The <seealso cref="IIdleStrategy"/> to be used when awaiting a response from the Media Driver.
            /// <para>
            /// This can be change to a <seealso cref="BusySpinIdleStrategy"/> or <seealso cref="YieldingIdleStrategy"/> for lower response time,
            /// especially for adding counters or releasing resources, at the expense of CPU usage.
            /// 
            /// </para>
            /// </summary>
            /// <returns> the <seealso cref="IIdleStrategy"/> to be used when awaiting a response from the Media Driver. </returns>
            public IIdleStrategy AwaitingIdleStrategy()
            {
                return _awaitingIdleStrategy;
            }

            /// <summary>
            /// This method is used for testing and debugging.
            /// </summary>
            /// <param name="toClientBuffer"> Injected CopyBroadcastReceiver </param>
            /// <returns> this for a fluent API. </returns>
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
            /// <returns> this for a fluent API. </returns>
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
            /// <seealso cref="Configuration.DEFAULT_ERROR_HANDLER"/>. This is the error handler which will be used if an error occurs
            /// during the callback for poll operations such as <seealso cref="Subscription.Poll(FragmentHandler, int)"/>.
            /// 
            /// The error handler can be reset after <seealso cref="Aeron.Connect()"/> and the latest version will always be used
            /// so that the boot-strapping process can be performed such as replacing the default one with a
            /// <seealso cref="CountedErrorHandler"/>.
            /// </summary>
            /// <param name="errorHandler"> Method to handle objects of type Throwable. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="DriverTimeoutException" />
            /// <seealso cref="RegistrationException" />
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                _errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the error handler that will be called for errors reported back from the media driver or during poll operations.
            /// </summary>
            /// <returns> the error handler that will be called for errors reported back from the media driver or during poll operations. </returns>
            public ErrorHandler ErrorHandler()
            {
                return _errorHandler;
            }

            /// <summary>
            /// The error handler which will be used if an error occurs during the callback for poll operations such as
            /// <seealso cref="Subscription.Poll(FragmentHandler, int)"/>. The default will be <seealso cref="ErrorHandler()"/> if not set.
            /// </summary>
            /// <param name="errorHandler"> Method to handle objects of type Throwable. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="DriverTimeoutException"/>
            /// <seealso cref="RegistrationException"/>
            public Context SubscriberErrorHandler(ErrorHandler errorHandler)
            {
                _subscriberErrorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// This is the error handler which will be used if an error occurs during the callback for poll operations
            /// such as <seealso cref="Subscription.Poll(FragmentHandler, int)"/>. The default will be <seealso cref="ErrorHandler()"/> if not
            /// set. To have <seealso cref="Subscription.Poll(FragmentHandler, int)"/> not delegate then set with
            /// <seealso cref="RethrowingErrorHandler"/>.
            /// </summary>
            /// <returns> the error handler that will be called for errors reported back from the media driver. </returns>
            public ErrorHandler SubscriberErrorHandler()
            {
                return _subscriberErrorHandler;
            }

            /// <summary>
            /// Setup a default callback for when an <seealso cref="Image"/> is available.
            /// </summary>
            /// <param name="handler"> Callback method for handling available image notifications. </param>
            /// <returns> this for a fluent API. </returns>
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
            /// <returns> this for a fluent API. </returns>
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
            /// Setup a callback for when a counter is available. This will be added to the list first before
            /// additional handler are added with <seealso cref="Aeron.AddAvailableCounterHandler"/>.
            /// </summary>
            /// <param name="handler"> to be called for handling available counter notifications. </param>
            /// <returns> this for a fluent API. </returns>
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
            /// Setup a callback for when a counter is unavailable. This will be added to the list first before
            /// additional handler are added with <seealso cref="Aeron.AddUnavailableCounterHandler"/>.
            /// </summary>
            /// <param name="handler"> to be called for handling unavailable counter notifications. </param>
            /// <returns> this for a fluent API. </returns>
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
            /// Set a <seealso cref="Action"/> that is called when the client is closed by timeout or normal means.
            ///        
            /// It is not safe to call any API functions from any threads after this hook is called. In addition, any
            /// in flight calls may still cause faults. Thus treating this as a hard error and
            /// terminate the process in this hook as soon as possible is recommended.
            /// </summary>
            /// <param name="handler"> that is called when the client is closed. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CloseHandler(Action handler)
            {
                this._closeHandler = handler;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Action"/> that is called when the client is closed by timeout or normal means.
            /// </summary>
            /// <returns> the <seealso cref="Action"/> that is called when the client is closed. </returns>
            public Action CloseHandler()
            {
                return _closeHandler;
            }

            /// <summary>
            /// Get the buffer containing the counter metadata. These counters are R/W for the driver, read only for all
            /// other users.
            /// </summary>
            /// <returns> The buffer storing the counter metadata. </returns>
            public UnsafeBuffer CountersMetaDataBuffer()
            {
                return _countersMetaDataBuffer;
            }

            /// <summary>
            /// Set the buffer containing the counter metadata. Testing/internal purposes only.
            /// </summary>
            /// <param name="countersMetaDataBuffer"> The new counter metadata buffer. </param>
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
            public Context KeepAliveIntervalNs(long value)
            {
                _keepAliveIntervalNs = value;
                return this;
            }

            /// <summary>
            /// Get the interval in nanoseconds for which the client will perform keep-alive operations.
            /// </summary>
            /// <returns> the interval in nanoseconds for which the client will perform keep-alive operations. </returns>
            public long KeepAliveIntervalNs()
            {
                return _keepAliveIntervalNs;
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
                return CheckDebugTimeout(_driverTimeoutMs, TimeUnit.MILLIS, nameof(DriverTimeoutMs));
            }

            private static readonly ConcurrentDictionary<string, bool> DebugFieldsSeen =
                new ConcurrentDictionary<string, bool>();

            /// <summary>
            /// Override the supplied timeout with the debug value if it has been set, and we are in debug mode.
            /// </summary>
            /// <param name="timeout">  The timeout value currently in use. </param>
            /// <param name="timeUnit"> The units of the timeout value. Debug timeout is specified in ns, so will be converted to this
            ///                 unit. </param>
            /// <param name="debugFieldName"> The field name to be added to the map.</param>
            /// <returns> The debug timeout if specified, and we are being debugged or the supplied value if not. Will be in
            /// timeUnit units. </returns>
            public static long CheckDebugTimeout(long timeout, TimeUnit timeUnit, string debugFieldName)
            {
                string debugTimeoutString = Config.GetProperty(DEBUG_TIMEOUT_PROP_NAME);
                if (null == debugTimeoutString || !Debugger.IsAttached)
                {
                    return timeout;
                }

                try
                {
                    long debugTimeoutNs = SystemUtil.ParseDuration(DEBUG_TIMEOUT_PROP_NAME, debugTimeoutString);
                    long debugTimeout = timeUnit.Convert(debugTimeoutNs, TimeUnit.NANOSECONDS);
                    if (DebugFieldsSeen.TryAdd(debugFieldName, true))
                    {
                        string message = "Using debug timeout [" + debugTimeout + "] for " + debugFieldName +
                                         " replacing [" + timeout + "]";
                        Console.WriteLine(message);
                    }

                    return debugTimeout;
                }
                catch (FormatException)
                {
                    return timeout;
                }
            }

            /// <summary>
            /// Set the timeout between service calls the to <seealso cref="ClientConductor"/> duty cycles in nanoseconds.
            ///
            /// Note: this method is used for testing only.
            /// </summary>
            /// <param name="interServiceTimeout"> the timeout (ns) between service calls the to <seealso cref="ClientConductor"/> duty cycle. </param>
            /// <returns> this Aeron.Context for method chaining. </returns>
            internal Context InterServiceTimeoutNs(long interServiceTimeout)
            {
                _interServiceTimeoutNs = interServiceTimeout;
                return this;
            }

            /// <summary>
            /// Return the timeout between service calls to the duty cycle for the client.
            /// <para>
            /// When exceeded, <seealso cref="Agrona.ErrorHandler"/> will be called and the active <seealso cref="Publication"/>s, <seealso cref="Image"/>s,
            /// and <seealso cref="Counter"/>s will be closed.
            /// </para>
            /// <para>
            /// This value is controlled by the driver and included in the CnC file. It can be configured by adjusting
            /// the <code>aeron.client.liveness.timeout</code> property set on the media driver.
            /// </para>
            /// </summary>
            /// <returns> the timeout between service calls in nanoseconds. </returns>
            public long InterServiceTimeoutNs()
            {
                return CheckDebugTimeout(_interServiceTimeoutNs, TimeUnit.NANOSECONDS, nameof(InterServiceTimeoutNs));
            }

            /// <summary>
            /// Duration to wait while lingering an entity such as an <seealso cref="Image"/> before deleting underlying resources
            /// such as memory mapped files.
            /// </summary>
            /// <param name="resourceLingerDurationNs"> to wait before deleting an expired resource. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.RESOURCE_LINGER_DURATION_PROP_NAME"></seealso>
            public Context ResourceLingerDurationNs(long resourceLingerDurationNs)
            {
                _resourceLingerDurationNs = resourceLingerDurationNs;
                return this;
            }

            /// <summary>
            /// Duration to wait while lingering an entity such as an <seealso cref="Image"/> before deleting underlying resources
            /// such as memory mapped files.
            /// </summary>
            /// <returns> duration in nanoseconds to wait before deleting an expired resource. </returns>
            /// <seealso cref="Configuration.RESOURCE_LINGER_DURATION_PROP_NAME"></seealso>
            public long ResourceLingerDurationNs()
            {
                return _resourceLingerDurationNs;
            }

            /// <summary>
            /// Duration to linger on closing to allow publishers and subscribers time to notice closed resources.
            /// <para>
            /// This value can be increased from the default to a few seconds to better cope with long GC pauses
            /// or resource starved environments. Issues could manifest as seg faults using files after they have
            /// been unmapped from publishers or subscribers not noticing the close in a timely fashion.
            ///        
            /// </para>
            /// </summary>
            /// <param name="closeLingerDurationNs"> to wait before deleting resources when closing. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLOSE_LINGER_DURATION_PROP_NAME"/>
            public Context CloseLingerDurationNs(long closeLingerDurationNs)
            {
                _closeLingerDurationNs = closeLingerDurationNs;
                return this;
            }

            /// <summary>
            /// Duration to linger on closing to allow publishers and subscribers time to notice closed resources.
            /// <para>
            /// This value can be increased from the default to a few seconds to better cope with long GC pauses
            /// or resource starved environments. Issues could manifest as seg faults using files after they have
            /// been unmapped from publishers or subscribers not noticing the close in a timely fashion.
            /// 
            /// </para>
            /// </summary>
            /// <returns> duration in nanoseconds to wait before deleting resources when closing. </returns>
            /// <seealso cref="Configuration.CLOSE_LINGER_DURATION_PROP_NAME"/>
            public long CloseLingerDurationNs()
            {
                return _closeLingerDurationNs;
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
            /// This is valid after a call to <seealso cref="Conclude()"/> or <see cref="ConcludeAeronDirectory()"/>.
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
            /// <returns> this for a fluent API. </returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                _threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// The thread factory to be used to construct the conductor thread
            /// </summary>
            /// <returns>the specified thread factory of <see cref="DefaultThreadFactory"/> if none is provided. </returns>
            public IThreadFactory ThreadFactory()
            {
                return _threadFactory;
            }

            /// <summary>
            /// Clean up all resources that the client uses to communicate with the Media Driver.
            /// </summary>
            public void Dispose()
            {
                _cncByteBuffer?.Dispose();
                _cncByteBuffer = null;

                _cncMetaDataBuffer?.Dispose();
                _countersMetaDataBuffer?.Dispose();
                _countersValuesBuffer?.Dispose();
                _cncByteBuffer?.Dispose();
            }

            /// <summary>
            /// {@inheritDoc}
            /// </summary>
            public override string ToString()
            {
                return "Aeron.Context" +
                       "\n{" +
                       "\n    isConcluded=" + _isConcluded +
                       "\n    aeronDirectory=" + AeronDirectory() +
                       "\n    aeronDirectoryName='" + AeronDirectoryName() + '\'' +
                       "\n    cncFile=" + CncFile() +
                       "\n    countersMetaDataBuffer=" + CountersMetaDataBuffer() +
                       "\n    countersValuesBuffer=" + CountersValuesBuffer() +
                       "\n    driverTimeoutMs=" + DriverTimeoutMs() +
                       "\n    clientId=" + _clientId +
                       "\n    useConductorAgentInvoker=" + _useConductorAgentInvoker +
                       "\n    preTouchMappedMemory=" + _preTouchMappedMemory +
                       "\n    driverAgentInvoker=" + _driverAgentInvoker +
                       "\n    clientLock=" + _clientLock +
                       "\n    epochClock=" + _epochClock +
                       "\n    nanoClock=" + _nanoClock +
                       "\n    idleStrategy=" + _idleStrategy +
                       "\n    awaitingIdleStrategy=" + _awaitingIdleStrategy +
                       "\n    toClientBuffer=" + _toClientBuffer +
                       "\n    toDriverBuffer=" + _toDriverBuffer +
                       "\n    driverProxy=" + _driverProxy +
                       "\n    cncByteBuffer=" + _cncByteBuffer +
                       "\n    cncMetaDataBuffer=" + _cncMetaDataBuffer +
                       "\n    logBuffersFactory=" + _logBuffersFactory +
                       "\n    errorHandler=" + _errorHandler +
                       "\n    subscriberErrorHandler=" + _subscriberErrorHandler +
                       "\n    availableImageHandler=" + _availableImageHandler +
                       "\n    unavailableImageHandler=" + _unavailableImageHandler +
                       "\n    availableCounterHandler=" + _availableCounterHandler +
                       "\n    unavailableCounterHandler=" + _unavailableCounterHandler +
                       "\n    closeHandler=" + _closeHandler +
                       "\n    keepAliveIntervalNs=" + _keepAliveIntervalNs +
                       "\n    interServiceTimeoutNs=" + _interServiceTimeoutNs +
                       "\n    resourceLingerDurationNs=" + _resourceLingerDurationNs +
                       "\n    closeLingerDurationNs=" + _closeLingerDurationNs +
                       "\n    threadFactory=" + _threadFactory +
                       "\n}";
            }

            private void ConnectToDriver()
            {
                long deadLineMs = _epochClock.Time() + DriverTimeoutMs();
                FileInfo cncFile = CncFile();

                while (null == _toDriverBuffer)
                {
                    cncFile.Refresh();

                    _cncByteBuffer = WaitForFileMapping(cncFile, _epochClock, deadLineMs);
                    _cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(_cncByteBuffer);

                    int cncVersion;
                    while (0 == (cncVersion = _cncMetaDataBuffer.GetIntVolatile(CncFileDescriptor.CncVersionOffset(0))))
                    {
                        if (_epochClock.Time() > deadLineMs)
                        {
                            throw new DriverTimeoutException("CnC file is created but not initialised");
                        }

                        Sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
                    }

                    CncFileDescriptor.CheckVersion(cncVersion);

                    if (SemanticVersion.Minor(cncVersion) < SemanticVersion.Minor(CncFileDescriptor.CNC_VERSION))
                    {
                        throw new AeronException("driverVersion=" + SemanticVersion.ToString(cncVersion) +
                                                 " insufficient for clientVersion=" +
                                                 SemanticVersion.ToString(CncFileDescriptor.CNC_VERSION));
                    }

                    if (!CncFileDescriptor.IsCncFileLengthSufficient(_cncMetaDataBuffer, _cncByteBuffer.Capacity))
                    {
                        _cncByteBuffer?.Dispose();
                        _cncByteBuffer = null;
                        _cncMetaDataBuffer = null;

                        Sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
                        continue;
                    }

                    ManyToOneRingBuffer ringBuffer =
                        new ManyToOneRingBuffer(
                            CncFileDescriptor.CreateToDriverBuffer(_cncByteBuffer, _cncMetaDataBuffer));

                    while (0 == ringBuffer.ConsumerHeartbeatTime())
                    {
                        if (_epochClock.Time() > deadLineMs)
                        {
                            throw new DriverTimeoutException("no driver heartbeat detected.");
                        }

                        Sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
                    }

                    long timeMs = _epochClock.Time();
                    if (ringBuffer.ConsumerHeartbeatTime() < (timeMs - DriverTimeoutMs()))
                    {
                        if (timeMs > deadLineMs)
                        {
                            throw new DriverTimeoutException("no driver heartbeat detected.");
                        }

                        IoUtil.Unmap(_cncByteBuffer);
                        _cncByteBuffer = null;
                        _cncMetaDataBuffer = null;

                        Sleep(100);
                        continue;
                    }

                    _toDriverBuffer = ringBuffer;
                }
            }

            private static MappedByteBuffer WaitForFileMapping(FileInfo cncFile,
                IEpochClock clock, long deadLineMs)
            {
                while (true)
                {
                    while (!cncFile.Exists || cncFile.Length <= 0)
                    {
                        if (clock.Time() > deadLineMs)
                        {
                            throw new DriverTimeoutException("CnC file not created: " + cncFile.FullName);
                        }

                        Sleep(Configuration.IdleSleepMs);

                        cncFile.Refresh();
                    }

                    try
                    {
                        var fileAccess = FileAccess.ReadWrite;
                        var fileShare = FileShare.ReadWrite | FileShare.Delete;

                        var fileStream = cncFile.Open(FileMode.Open, fileAccess, fileShare);
                        var fileSize = fileStream.Length;

                        if (fileSize < CncFileDescriptor.META_DATA_LENGTH)
                        {
                            if (clock.Time() > deadLineMs)
                            {
                                throw new DriverTimeoutException("CnC file is created but not populated.");
                            }

                            fileStream.Dispose();
                            Sleep(Configuration.IdleSleepMs);
                            continue;
                        }

                        return IoUtil.MapExistingFile(fileStream);
                    }
                    catch (FileNotFoundException)
                    {
                    }
                    catch (IOException)
                    {
                    }
                    catch (Exception ex)
                    {
                        throw new AeronException("cannot open CnC file", ex);
                    }
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

                if (cncFile.Exists && cncFile.Length > 0)
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

                if (cncFile.Exists && cncFile.Length > 0)
                {
                    logger("INFO: Aeron CnC file " + cncFile + " exists");

                    var cncByteBuffer = IoUtil.MapExistingFile(cncFile, "CnC file");
                    try
                    {
                        return IsDriverActive(driverTimeoutMs, logger, cncByteBuffer);
                    }
                    finally
                    {
                        cncByteBuffer?.Dispose();
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
                    cncByteBuffer?.Dispose();
                }
            }

            /// <summary>
            /// Is a media driver active in the current mapped CnC buffer? If the driver is starting then it will wait for
            /// up to the driverTimeoutMs by checking for the cncVersion being set.
            /// </summary>
            /// <param name="driverTimeoutMs"> for the driver liveness check. </param>
            /// <param name="logger">          for feedback as liveness checked. </param>
            /// <param name="cncByteBuffer">   for the existing CnC file. </param>
            /// <returns> true if a driver is active or false if not. </returns>
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

                CncFileDescriptor.CheckVersion(cncVersion);

                ManyToOneRingBuffer toDriverBuffer =
                    new ManyToOneRingBuffer(CncFileDescriptor.CreateToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                long timestampMs = toDriverBuffer.ConsumerHeartbeatTime();
                long nowMs = DateTime.Now.ToFileTimeUtc();
                long timestampAgeMs = nowMs - timestampMs;

                logger("INFO: Aeron toDriver consumer heartbeat is (ms):" + timestampAgeMs);

                return timestampAgeMs <= driverTimeoutMs;
            }

            /// <summary>
            /// Request a driver to run its termination hook.
            /// </summary>
            /// <param name="directory"> for the driver. </param>
            /// <param name="tokenBuffer"> containing the optional token for the request. </param>
            /// <param name="tokenOffset"> within the tokenBuffer at which the token begins. </param>
            /// <param name="tokenLength"> of the token in the tokenBuffer. </param>
            /// <returns> true if request was sent or false if request could not be sent. </returns>
            public static bool RequestDriverTermination(
                DirectoryInfo directory,
                IDirectBuffer tokenBuffer,
                int tokenOffset,
                int tokenLength)
            {
                FileInfo cncFile = new FileInfo(Path.Combine(directory.FullName, CncFileDescriptor.CNC_FILE));

                if (cncFile.Exists && cncFile.Length > 0)
                {
                    var cncByteBuffer = IoUtil.MapExistingFile(cncFile, "CnC file");
                    try
                    {
                        UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);
                        int cncVersion = cncMetaDataBuffer.GetIntVolatile(CncFileDescriptor.CncVersionOffset(0));

                        CncFileDescriptor.CheckVersion(cncVersion);

                        ManyToOneRingBuffer toDriverBuffer =
                            new ManyToOneRingBuffer(
                                CncFileDescriptor.CreateToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
                        long clientId = toDriverBuffer.NextCorrelationId();
                        DriverProxy driverProxy = new DriverProxy(toDriverBuffer, clientId);

                        return driverProxy.TerminateDriver(tokenBuffer, tokenOffset, tokenLength);
                    }
                    finally
                    {
                        cncByteBuffer?.Dispose();
                    }
                }

                return false;
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
                    cncByteBuffer?.Dispose();
                }
            }

            /// <summary>
            /// Read the error log to a given <seealso cref="StreamWriter"/>
            /// </summary>
            /// <param name="writer"> to write the error log contents to. </param>
            /// <param name="cncByteBuffer"> containing the error log.</param>
            /// <returns> the number of observations from the error log </returns>
            public int SaveErrorLog(StreamWriter writer, MappedByteBuffer cncByteBuffer)
            {
                if (null == cncByteBuffer)
                {
                    return 0;
                }

                return PrintErrorLog(ErrorLogBuffer(cncByteBuffer), writer);
            }

            /// <summary>
            /// Print the contents of an error log to a <seealso cref="TextWriter"/> in human-readable format.
            /// </summary>
            /// <param name="errorBuffer"> to read errors from. </param>
            /// <param name="out">         print the errors to. </param>
            /// <returns> number of distinct errors observed. </returns>
            public static int PrintErrorLog(IAtomicBuffer errorBuffer, TextWriter @out)
            {
                int distinctErrorCount = 0;
                if (ErrorLogReader.HasErrors(errorBuffer))
                {
                    void ErrorConsumer(int count, long firstTimestamp, long lastTimestamp, string ex)
                        => @out.WriteLine(
                            $"***{Environment.NewLine}{count} observations from {new DateTime(firstTimestamp)} " +
                            $"to {new DateTime(lastTimestamp)} " +
                            $"for:{Environment.NewLine} {ex}");

                    distinctErrorCount = ErrorLogReader.Read(errorBuffer, ErrorConsumer);
                }

                @out.WriteLine();
                @out.WriteLine("{0} distinct errors observed.", distinctErrorCount);

                return distinctErrorCount;
            }

            private static IAtomicBuffer ErrorLogBuffer(MappedByteBuffer cncByteBuffer)
            {
                UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);
                int cncVersion = cncMetaDataBuffer.GetInt(CncFileDescriptor.CncVersionOffset(0));

                CncFileDescriptor.CheckVersion(cncVersion);

                return CncFileDescriptor.CreateErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
            }

            /// <summary>
            /// Wrap a user ErrorHandler so that error will continue to write to the errorLog.
            /// </summary>
            /// <param name="userErrorHandler"> the user specified ErrorHandler, can be null. </param>
            /// <param name="errorLog">         the configured errorLog, either the default or user supplied. </param>
            /// <returns> an error handler that will delegate to both the userErrorHandler and the errorLog. </returns>
            public static ErrorHandler SetupErrorHandler(ErrorHandler userErrorHandler, DistinctErrorLog errorLog)
            {
                LoggingErrorHandler loggingErrorHandler = new LoggingErrorHandler(errorLog);
                if (null == userErrorHandler)
                {
                    return loggingErrorHandler.OnError;
                }
                else
                {
                    return (throwable) =>
                    {
                        loggingErrorHandler.OnError(throwable);
                        userErrorHandler(throwable);
                    };
                }
            }
        }

        private static void Sleep(int durationMs)
        {
            try
            {
                Thread.Sleep(durationMs);
            }
            catch (ThreadInterruptedException ex)
            {
                Thread.CurrentThread.Interrupt();
                throw new AeronException("unexpected interrupt", ex);
            }
        }
    }
}
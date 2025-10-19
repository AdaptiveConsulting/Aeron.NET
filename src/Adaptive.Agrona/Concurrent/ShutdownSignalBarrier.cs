using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// One time barrier for blocking one or more threads until a SIGINT or SIGTERM signal is received from the operating
    /// system or by programmatically calling <seealso cref="Signal"/>. Useful for shutting down a service.
    /// </summary>
    public sealed class ShutdownSignalBarrier : IDisposable
    {
        /// <summary>Delegate notified when the barrier is signaled.</summary>
        public delegate void SignalHandler();

        private static readonly SignalHandler NO_OP_SIGNAL_HANDLER = () => { };

        // Registry of all active barriers (value is unused).
        private static readonly ConcurrentDictionary<ShutdownSignalBarrier, byte> BARRIERS =
            new ConcurrentDictionary<ShutdownSignalBarrier, byte>();

        // Static ctor: hook process termination and Ctrl+C, like Java's shutdown hook.
        static ShutdownSignalBarrier()
        {
            AppDomain.CurrentDomain.ProcessExit += (sender, args) =>
            {
                var barriers = SignalAndClearAll();
                AwaitTermination(barriers, TimeSpan.FromSeconds(10), Console.Out);
            };

            Console.CancelKeyPress += (sender, e) =>
            {
                var barriers = SignalAndClearAll();
                AwaitTermination(barriers, TimeSpan.FromSeconds(10), Console.Out);
                e.Cancel = false; // allow process to exit
            };
        }

        // Latches: wait for signal, and wait for "close" acknowledgement.
        private readonly ManualResetEventSlim _waitEvent = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim _closeEvent = new ManualResetEventSlim(false);

        // Atomic boolean: 0 = not signaled, 1 = signaled
        private int _signaled = 0;

        private readonly SignalHandler _signalHandler;

        /// <summary>Construct and register the barrier ready for use.</summary>
        public ShutdownSignalBarrier() : this(NO_OP_SIGNAL_HANDLER)
        {
        }

        /// <summary>Construct and register the barrier with a signal handler.</summary>
        public ShutdownSignalBarrier(SignalHandler signalHandler)
        {
            if (signalHandler == null) throw new ArgumentNullException(nameof(signalHandler));
            _signalHandler = signalHandler;
            BARRIERS.TryAdd(this, 0);
        }

        /// <summary>Programmatically signal awaiting thread on this barrier.</summary>
        public void Signal()
        {
            if (Interlocked.CompareExchange(ref _signaled, 1, 0) == 0)
            {
                BARRIERS.TryRemove(this, out _);
                _waitEvent.Set();
                _signalHandler();
            }
        }

        /// <summary>Programmatically signal all awaiting threads.</summary>
        public void SignalAll()
        {
            SignalAndClearAll();
        }

        /// <summary>Remove this barrier from global shutdown handling.</summary>
        public void Remove()
        {
            BARRIERS.TryRemove(this, out _);
        }

        /// <summary>Await the reception of the shutdown signal.</summary>
        public void Await()
        {
            try
            {
                _waitEvent.Wait();
            }
            catch (ThreadInterruptedException)
            {
                try
                {
                    Signal();
                }
                finally
                {
                    // Preserve interruption status.
                    Thread.CurrentThread.Interrupt();
                }
            }
        }

        /// <summary>Close this barrier to allow process termination.</summary>
        public void Close()
        {
            try
            {
                Signal();
            }
            finally
            {
                _closeEvent.Set();
            }
        }

        /// <summary>IDisposable implementation.</summary>
        public void Dispose()
        {
            Close();
        }

        public override string ToString()
        {
            return "ShutdownSignalBarrier{" +
                   "waitEvent=" + _waitEvent.IsSet +
                   ", closeEvent=" + _closeEvent.IsSet +
                   ", signaled=" + (_signaled == 1) +
                   "}";
        }

        // --------- Static helpers (Java's private static methods) ---------

        private static ShutdownSignalBarrier[] SignalAndClearAll()
        {
            // Snapshot then clear, like CopyOnWriteArrayList semantics.
            var snapshot = new ShutdownSignalBarrier[BARRIERS.Count];
            BARRIERS.Keys.CopyTo(snapshot, 0);
            BARRIERS.Clear();

            List<Exception> exceptions = null;

            for (int i = 0; i < snapshot.Length; i++)
            {
                try
                {
                    snapshot[i].Signal();
                }
                catch (Exception ex)
                {
                    if (exceptions == null) exceptions = new List<Exception>(4);
                    exceptions.Add(ex);
                }
            }

            if (exceptions != null && exceptions.Count > 0)
            {
                // Closest .NET equivalent to Java's suppressed exceptions.
                throw new AggregateException("One or more barriers threw during Signal()", exceptions);
            }

            return snapshot;
        }

        private static void AwaitTermination(
            ShutdownSignalBarrier[] barriers,
            TimeSpan timeoutPerBarrier,
            TextWriter output)
        {
            if (barriers == null || barriers.Length == 0) return;

            bool wasInterrupted = false;

            try
            {
                var remaining = (ShutdownSignalBarrier[])barriers.Clone();
                int completed = 0;

                do
                {
                    for (int i = 0; i < remaining.Length; i++)
                    {
                        var barrier = remaining[i];
                        if (barrier == null) continue;

                        try
                        {
                            if (barrier._closeEvent.Wait(timeoutPerBarrier))
                            {
                                completed++;
                                remaining[i] = null;
                            }
                            else
                            {
                                output.WriteLine(
                                    "WARN: ShutdownSignalBarrier hasn't terminated in " +
                                    timeoutPerBarrier.TotalSeconds.ToString("N0") +
                                    " seconds! Did you forget to call Close()/Dispose() on it?");
                            }
                        }
                        catch (ThreadInterruptedException)
                        {
                            wasInterrupted = true;
                            break;
                        }
                    }
                } while (completed < remaining.Length);
            }
            finally
            {
                if (wasInterrupted)
                {
                    Thread.CurrentThread.Interrupt();
                }
            }
        }
    }
}
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.Ping
{
    /// <summary>
    /// Tracker and reporter of rates.
    /// 
    /// Uses volatile semantics for counters.
    /// </summary>
    public class RateReporter
    {
        /// <summary>
        /// Interface for reporting of rate information
        /// </summary>
        public delegate void Reporter(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes);

        private readonly int ReportIntervalMS;
        private readonly Reporter ReportingFunc;

        private volatile bool Halt_Renamed = false;
        private long TotalBytes;
        private long TotalMessages;
        private long LastTotalBytes;
        private readonly Stopwatch _stopwatch;
        private long LastTotalMessages;


        /// <summary>
        /// Create a rate reporter with the given report interval in nanoseconds and the reporting function.
        /// </summary>
        /// <param name="reportInterval"> in nanoseconds </param>
        /// <param name="reportingFunc"> to call for reporting rates </param>
        public RateReporter(int reportInterval, Reporter reportingFunc)
        {
            ReportIntervalMS = reportInterval;
            ReportingFunc = reportingFunc;
            _stopwatch = Stopwatch.StartNew();
        }

        /// <summary>
        /// Run loop for the rate reporter
        /// </summary>
        public virtual void Run()
        {
            do
            {
                Thread.Sleep(ReportIntervalMS); // == Park?

                var currentTotalMessages = TotalMessages;
                var currentTotalBytes = TotalBytes;
                var timeSpanNs = _stopwatch.ElapsedMilliseconds;
                var messagesPerSec = (currentTotalMessages - LastTotalMessages)*ReportIntervalMS/(double) timeSpanNs;
                var bytesPerSec = (currentTotalBytes - LastTotalBytes)*ReportIntervalMS/(double) timeSpanNs;

                ReportingFunc(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

                LastTotalBytes = currentTotalBytes;
                LastTotalMessages = currentTotalMessages;
            } while (!Halt_Renamed);
        }

        /// <summary>
        /// Signal the run loop to exit. Does not block.
        /// </summary>
        public virtual void Halt()
        {
            Halt_Renamed = true;
        }

        /// <summary>
        /// Tell rate reporter of number of messages and bytes received, sent, etc.
        /// </summary>
        /// <param name="messages"> received, sent, etc. </param>
        /// <param name="bytes"> received, sent, etc. </param>
        public virtual void OnMessage(long messages, long bytes)
        {
            TotalBytes += bytes;
            TotalMessages += messages;
        }
    }

    /// <summary>
    /// Utility functions for samples
    /// </summary>
    public class SamplesUtil
    {
        /// <summary>
        /// Return a reusable, parameterised event loop that calls a default idler when no messages are received
        /// </summary>
        /// <param name="fragmentHandler"> to be called back for each message. </param>
        /// <param name="limit">           passed to <seealso cref="Subscription#poll(FragmentHandler, int)"/> </param>
        /// <param name="running">         indication for loop </param>
        /// <returns> loop function </returns>
        public static Action<Subscription> SubscriberLoop(IFragmentHandler fragmentHandler, int limit, AtomicBoolean running)
        {
            IIdleStrategy idleStrategy = new BusySpinIdleStrategy();

            return SubscriberLoop(fragmentHandler, limit, running, idleStrategy);
        }

        /// <summary>
        /// Return a reusable, parameterized event loop that calls and idler when no messages are received
        /// </summary>
        /// <param name="fragmentHandler"> to be called back for each message. </param>
        /// <param name="limit">           passed to <seealso cref="Subscription#poll(FragmentHandler, int)"/> </param>
        /// <param name="running">         indication for loop </param>
        /// <param name="idleStrategy">    to use for loop </param>
        /// <returns> loop function </returns>
        public static Action<Subscription> SubscriberLoop(IFragmentHandler fragmentHandler, int limit, AtomicBoolean running, IIdleStrategy idleStrategy)
        {
            return subscription =>
            {
                while (running.Get())
                {
                    idleStrategy.Idle(subscription.Poll(fragmentHandler, limit));
                }
            };
        }

        /// <summary>
        /// Return a reusable, parameterized <seealso cref="FragmentHandler"/> that prints to stdout
        /// </summary>
        /// <param name="streamId"> to show when printing </param>
        /// <returns> subscription data handler function that prints the message contents </returns>
        public static IFragmentHandler PrintStringMessage(int streamId)
        {
            return new DelegateFragmentHandler((buffer, offset, length, header) =>
            {
                var data = new byte[length];
                buffer.GetBytes(offset, data);

                Console.WriteLine($"Message to stream {streamId:D} from session {header.SessionId():D} ({length:D}@{offset:D}) <<{Encoding.UTF8.GetString(data)}>>");
            });
        }

        /// <summary>
        /// Return a reusable, parameteried <seealso cref="FragmentHandler"/> that calls into a
        /// <seealso cref="RateReporter"/>.
        /// </summary>
        /// <param name="reporter"> for the rate </param>
        /// <returns> <seealso cref="FragmentHandler"/> that records the rate information </returns>
        public static IFragmentHandler RateReporterHandler(RateReporter reporter)
        {
            return new DelegateFragmentHandler((buffer, offset, length, header) => reporter.OnMessage(1, length));
        }

        /// <summary>
        /// Generic error handler that just prints message to stdout.
        /// </summary>
        /// <param name="channel">   for the error </param>
        /// <param name="streamId">  for the error </param>
        /// <param name="sessionId"> for the error, if source </param>
        /// <param name="message">   indicating what the error was </param>
        /// <param name="cause">     of the error </param>
        public static void PrintError(string channel, int streamId, int sessionId, string message, HeaderFlyweight cause)
        {
            Console.WriteLine(message);
        }

        /// <summary>
        /// Print the rates to stdout
        /// </summary>
        /// <param name="messagesPerSec"> being reported </param>
        /// <param name="bytesPerSec">    being reported </param>
        /// <param name="totalMessages">  being reported </param>
        /// <param name="totalBytes">     being reported </param>
        public static void PrintRate(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes)
        {
            Console.WriteLine($"{messagesPerSec:g02} msgs/sec, {bytesPerSec:g02} bytes/sec, totals {totalMessages:D} messages {totalBytes/(1024*1024):D} MB");
        }

        /// <summary>
        /// Print the information for an available image to stdout.
        /// </summary>
        /// <param name="image"> that has been created </param>
        public static void PrintAvailableImage(Image image)
        {
            var subscription = image.Subscription();
            Console.WriteLine($"Available image on {subscription.Channel()} streamId={subscription.StreamId():D} sessionId={image.SessionId():D} from {image.SourceIdentity()}");
        }

        /// <summary>
        /// Print the information for an unavailable image to stdout.
        /// </summary>
        /// <param name="image"> that has gone inactive </param>
        public static void PrintUnavailableImage(Image image)
        {
            var subscription = image.Subscription();
            Console.WriteLine($"Unavailable image on {subscription.Channel()} streamId={subscription.StreamId():D} sessionId={image.SessionId():D}");
        }
    }

    public class DelegateFragmentHandler : IFragmentHandler
    {
        private readonly Action<IDirectBuffer, int, int, Header> _action;

        public DelegateFragmentHandler(Action<IDirectBuffer, int, int, Header> action)
        {
            _action = action;
        }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _action(buffer, offset, length, header);
        }
    }
}
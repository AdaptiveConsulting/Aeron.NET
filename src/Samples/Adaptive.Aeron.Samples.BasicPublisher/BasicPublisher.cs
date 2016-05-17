using System;
using System.Text;
using System.Threading;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.BasicPublisher
{
    /// <summary>
    /// Basic Aeron publisher application
    /// This publisher sends a fixed number of fixed-length messages
    /// on a channel and stream ID, then lingers to allow any consumers
    /// that may have experienced loss a chance to NAK for and recover
    /// any missing data.
    /// The default values for number of messages, channel, and stream ID are
    /// defined in <seealso cref="SampleConfiguration"/> and can be overridden by
    /// setting their corresponding properties via the command-line; e.g.:
    /// -Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20
    /// </summary>
    public class BasicPublisher
    {
        private static readonly int StreamID = SampleConfiguration.STREAM_ID;
        private static readonly string Channel = SampleConfiguration.CHANNEL;
        private static readonly long NumberOfMessages = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LingerTimeoutMs = SampleConfiguration.LINGER_TIMEOUT_MS;

        public static void Main()
        {
            Console.WriteLine("Publishing to " + Channel + " on stream Id " + StreamID);

            var ctx = new Aeron.Context();

            // Connect a new Aeron instance to the media driver and create a publication on
            // the given channel and stream ID.
            // The Aeron and Publication classes implement "AutoCloseable" and will automatically
            // clean up resources when this try block is finished
            using (var aeron = Aeron.Connect(ctx))
            using (var publication = aeron.AddPublication(Channel, StreamID))
            using (var buffer = new UnsafeBuffer(new byte[256]))
            {
                Thread.Sleep(100);

                for (var i = 0; i < NumberOfMessages; i++)
                {
                    var message = "Hello World! " + i;
                    var messageBytes = Encoding.UTF8.GetBytes(message);
                    buffer.PutBytes(0, messageBytes);

                    Console.WriteLine("offering " + i + "/" + NumberOfMessages);

                    var result = publication.Offer(buffer, 0, messageBytes.Length);

                    if (result < 0L)
                    {
                        if (result == Publication.BACK_PRESSURED)
                        {
                            Console.WriteLine("Offer failed due to back pressure");
                        }
                        else if (result == Publication.NOT_CONNECTED)
                        {
                            Console.WriteLine("Offer failed because publisher is not yet connected to subscriber");
                        }
                        else if (result == Publication.ADMIN_ACTION)
                        {
                            Console.WriteLine("Offer failed because of an administration action in the system");
                        }
                        else if (result == Publication.CLOSED)
                        {
                            Console.WriteLine("Offer failed publication is closed");
                            break;
                        }
                        else
                        {
                            Console.WriteLine("Offer failed due to unknown reason");
                        }
                    }
                    else
                    {
                        Console.WriteLine("yay!");
                    }

                    if (!publication.IsConnected)
                    {
                        Console.WriteLine("No active subscribers detected");
                    }
                }

                Console.WriteLine("Done sending.");

                if (0 < LingerTimeoutMs)
                {
                    Console.WriteLine("Lingering for " + LingerTimeoutMs + " milliseconds...");
                    Thread.Sleep((int) LingerTimeoutMs);
                }
            }
        }
    }
}
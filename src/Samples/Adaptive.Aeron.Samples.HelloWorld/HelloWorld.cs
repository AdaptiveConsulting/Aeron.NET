/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.HelloWorld
{
    public class HelloWorld
    {
        public static void Main()
        {
            const string channel = "aeron:ipc";
            const int streamId = 42;

            var buffer = new UnsafeBuffer(new byte[256]);

            try
            {
                using (var aeron = Aeron.Connect())
                using (var publisher = aeron.AddPublication(channel, streamId))
                using (var subscriber = aeron.AddSubscription(channel, streamId))
                {
                    var message = buffer.PutStringWithoutLengthUtf8(0, "Hello World!");

                    publisher.Offer(buffer, 0, message);
                    Console.WriteLine("Message sent...");

                    while (subscriber.Poll(PrintMessage, 1) == 0)
                    {
                        Thread.Sleep(10);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
            }
        }

        static void PrintMessage(IDirectBuffer buffer, int offset, int length, Header header)
        {
            var message = buffer.GetStringWithoutLengthUtf8(offset, length);

            Console.WriteLine($"Received message ({message}) to stream {header.StreamId:D} from session {header.SessionId:x} term id {header.TermId:x} term offset {header.TermOffset:D} ({length:D}@{offset:D})");
        }
    }
}
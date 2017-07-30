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

namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// Configuration used for samples all in one place.
    /// </summary>
    public class SampleConfiguration
    {
        private const string ChannelProp = "aeron.sample.channel";
        private const string StreamIDProp = "aeron.sample.streamId";

        private const string PingChannelProp = "aeron.sample.ping.channel";
        private const string PongChannelProp = "aeron.sample.pong.channel";
        private const string PingStreamIDProp = "aeron.sample.ping.streamId";
        private const string PongStreamIDProp = "aeron.sample.pong.streamId";
        private const string WarmupNumberOfMessagesProp = "aeron.sample.warmup.messages";
        private const string WarmupNumberOfIterationsProp = "aeron.sample.warmup.iterations";
        private const string RandomMessageLengthProp = "aeron.sample.randomMessageLength";

        private const string FrameCountLimitProp = "aeron.sample.frameCountLimit";
        private const string MessageLengthProp = "aeron.sample.messageLength";
        private const string NumberOfMessagesProp = "aeron.sample.messages";
        private const string LingerTimeoutMsProp = "aeron.sample.lingerTimeout";
        
        public static readonly string CHANNEL;
        public static readonly string PING_CHANNEL;
        public static readonly string PONG_CHANNEL;
        public static readonly int STREAM_ID;
        public static readonly int PING_STREAM_ID;
        public static readonly int PONG_STREAM_ID;
        public static readonly bool RANDOM_MESSAGE_LENGTH;
        public static readonly int FRAGMENT_COUNT_LIMIT;
        public static readonly int MESSAGE_LENGTH;
        public static readonly int NUMBER_OF_MESSAGES;
        public static readonly int WARMUP_NUMBER_OF_MESSAGES;
        public static readonly int WARMUP_NUMBER_OF_ITERATIONS;
        public static readonly long LINGER_TIMEOUT_MS;

        static SampleConfiguration()
        {
            CHANNEL = Config.GetProperty(ChannelProp, "aeron:udp?endpoint=localhost:40123");
            STREAM_ID = Config.GetInteger(StreamIDProp, 10);
            PING_CHANNEL = Config.GetProperty(PingChannelProp, "aeron:udp?endpoint=localhost:40123");
            PONG_CHANNEL = Config.GetProperty(PongChannelProp, "aeron:udp?endpoint=localhost:40124");
            PING_STREAM_ID = Config.GetInteger(PingStreamIDProp, 10);
            PONG_STREAM_ID = Config.GetInteger(PongStreamIDProp, 10);
            FRAGMENT_COUNT_LIMIT = Config.GetInteger(FrameCountLimitProp, 256);
            MESSAGE_LENGTH = Config.GetInteger(MessageLengthProp, 32);
            RANDOM_MESSAGE_LENGTH = Config.GetBoolean(RandomMessageLengthProp);
            NUMBER_OF_MESSAGES = Config.GetInteger(NumberOfMessagesProp, 1000000);
            WARMUP_NUMBER_OF_MESSAGES = Config.GetInteger(WarmupNumberOfMessagesProp, 10000);
            WARMUP_NUMBER_OF_ITERATIONS = Config.GetInteger(WarmupNumberOfIterationsProp, 5);
            LINGER_TIMEOUT_MS = Config.GetLong(LingerTimeoutMsProp, 5000);
        }
    }
}
namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// Configuration used for samples all in one place.
    /// </summary>
    public class SampleConfiguration
    {
        const string CHANNEL_PROP = "aeron.sample.channel";
        const string STREAM_ID_PROP = "aeron.sample.streamId";

        const string PING_CHANNEL_PROP = "aeron.sample.ping.channel";
        const string PONG_CHANNEL_PROP = "aeron.sample.pong.channel";
        const string PING_STREAM_ID_PROP = "aeron.sample.ping.streamId";
        const string PONG_STREAM_ID_PROP = "aeron.sample.pong.streamId";
        const string WARMUP_NUMBER_OF_MESSAGES_PROP = "aeron.sample.warmup.messages";
        const string WARMUP_NUMBER_OF_ITERATIONS_PROP = "aeron.sample.warmup.iterations";
        const string RANDOM_MESSAGE_LENGTH_PROP = "aeron.sample.randomMessageLength";

        const string FRAME_COUNT_LIMIT_PROP = "aeron.sample.frameCountLimit";
        const string MESSAGE_LENGTH_PROP = "aeron.sample.messageLength";
        const string NUMBER_OF_MESSAGES_PROP = "aeron.sample.messages";
        const string LINGER_TIMEOUT_MS_PROP = "aeron.sample.lingerTimeout";

        public static string INFO_FLAG_PROP = "aeron.sample.info";

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
        public static readonly bool INFO_FLAG;

        static SampleConfiguration()
        {
            CHANNEL = Config.GetProperty(CHANNEL_PROP, "aeron:udp?endpoint=localhost:40123");
            STREAM_ID = Config.GetInteger(STREAM_ID_PROP, 10);
            PING_CHANNEL = Config.GetProperty(PING_CHANNEL_PROP, "aeron:udp?endpoint=localhost:40123");
            PONG_CHANNEL = Config.GetProperty(PONG_CHANNEL_PROP, "aeron:udp?endpoint=localhost:40124");
            PING_STREAM_ID = Config.GetInteger(PING_STREAM_ID_PROP, 10);
            PONG_STREAM_ID = Config.GetInteger(PONG_STREAM_ID_PROP, 10);
            FRAGMENT_COUNT_LIMIT = Config.GetInteger(FRAME_COUNT_LIMIT_PROP, 256);
            MESSAGE_LENGTH = Config.GetInteger(MESSAGE_LENGTH_PROP, 256);
            RANDOM_MESSAGE_LENGTH = Config.GetBoolean(RANDOM_MESSAGE_LENGTH_PROP);
            NUMBER_OF_MESSAGES = Config.GetInteger(NUMBER_OF_MESSAGES_PROP, 1000000);
            WARMUP_NUMBER_OF_MESSAGES = Config.GetInteger(WARMUP_NUMBER_OF_MESSAGES_PROP, 10000);
            WARMUP_NUMBER_OF_ITERATIONS = Config.GetInteger(WARMUP_NUMBER_OF_ITERATIONS_PROP, 5);
            LINGER_TIMEOUT_MS = Config.GetLong(LINGER_TIMEOUT_MS_PROP, 5000);
            INFO_FLAG = Config.GetBoolean(INFO_FLAG_PROP);
        }
    }
}
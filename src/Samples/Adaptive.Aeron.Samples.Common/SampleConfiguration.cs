namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// Configuration used for samples all in one place.
    /// </summary>
    public class SampleConfiguration
    {
        public static readonly string CHANNEL = "aeron:ipc";
        public static readonly string PING_CHANNEL = "aeron:ipc";
        public static readonly string PONG_CHANNEL = "aeron:ipc";
        public static readonly int STREAM_ID = 10;
        public static readonly int PING_STREAM_ID = 10;
        public static readonly int PONG_STREAM_ID = 10;
        public static readonly int FRAGMENT_COUNT_LIMIT = 256;
        public static readonly int MESSAGE_LENGTH = 32;
        public static readonly int NUMBER_OF_MESSAGES = 1000000;
        public static readonly int WARMUP_NUMBER_OF_MESSAGES = 10000;
        public static readonly int WARMUP_NUMBER_OF_ITERATIONS = 5;
        public static readonly long LINGER_TIMEOUT_MS = 5000;
        public static readonly bool INFO_FLAG = true;
    }
}
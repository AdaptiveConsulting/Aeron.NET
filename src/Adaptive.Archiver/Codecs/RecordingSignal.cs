/* Generated SBE (Simple Binary Encoding) message codec */
namespace Adaptive.Archiver.Codecs {

    public enum RecordingSignal : int
    {
        START = 0,
        STOP = 1,
        EXTEND = 2,
        REPLICATE = 3,
        MERGE = 4,
        SYNC = 5,
        DELETE = 6,
        NULL_VALUE = -2147483648
    }
}

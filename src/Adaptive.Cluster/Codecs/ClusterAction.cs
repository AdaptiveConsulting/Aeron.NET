/* Generated SBE (Simple Binary Encoding) message codec */
namespace Adaptive.Cluster.Codecs {

    public enum ClusterAction : int
    {
        INIT = 0,
        SNAPSHOT = 1,
        READY = 2,
        REPLAY = 3,
        SUSPEND = 4,
        RESUME = 5,
        SHUTDOWN = 6,
        ABORT = 7,
        NULL_VALUE = -2147483648
    }
}

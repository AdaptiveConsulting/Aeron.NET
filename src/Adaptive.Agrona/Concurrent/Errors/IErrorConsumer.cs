namespace Adaptive.Agrona.Concurrent.Errors
{
    public delegate void ErrorConsumer(int observationCount, long firstObservationTimestamp, long lastObservationTimestamp, string encodedException);
}
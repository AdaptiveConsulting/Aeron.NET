namespace Adaptive.Agrona.Concurrent.Errors
{
    public interface IErrorConsumer
    {
        /// <summary>
        /// Callback for accepting errors encountered in the log.
        /// </summary>
        /// <param name="observationCount">          the number of times this distinct exception has been recorded. </param>
        /// <param name="firstObservationTimestamp"> time the first observation was recorded. </param>
        /// <param name="lastObservationTimestamp">  time the last observation was recorded. </param>
        /// <param name="encodedException">          String encoding of the exception and stack trace in UTF-8 format. </param>
        void Accept(int observationCount, long firstObservationTimestamp, long lastObservationTimestamp, string encodedException);
    }

}
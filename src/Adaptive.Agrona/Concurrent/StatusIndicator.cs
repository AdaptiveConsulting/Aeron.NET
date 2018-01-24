namespace Adaptive.Agrona.Concurrent
{
    public abstract class StatusIndicator : StatusIndicatorReader
    {
        /// <summary>
        /// Sets the current status indication of the component with ordered atomic memory semantics.
        /// </summary>
        /// <param name="value"> the current status indication of the component. </param>
        public abstract void SetOrdered(long value);
    }
}
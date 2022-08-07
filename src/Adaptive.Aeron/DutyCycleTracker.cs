namespace Adaptive.Aeron
{
	/// <summary>
	/// Tracker to handle tracking the duration of a duty cycle.
	/// </summary>
	public class DutyCycleTracker
	{
		private long timeOfLastUpdateNs;

		/// <summary>
		/// Update the last known clock time.
		/// </summary>
		/// <param name="nowNs"> to update with. </param>
		public void Update(long nowNs)
		{
			timeOfLastUpdateNs = nowNs;
		}

		/// <summary>
		/// Pass measurement to tracker and report updating last known clock time with time.
		/// </summary>
		/// <param name="nowNs"> of the measurement. </param>
		public void MeasureAndUpdate(long nowNs)
		{
			long cycleTimeNs = nowNs - timeOfLastUpdateNs;

			ReportMeasurement(cycleTimeNs);
			timeOfLastUpdateNs = nowNs;
		}

		/// <summary>
		/// Callback called to report duration of cycle.
		/// </summary>
		/// <param name="durationNs"> of the duty cycle. </param>
		public virtual void ReportMeasurement(long durationNs)
		{
		}
	}
}
namespace Adaptive.Agrona
{
	/// <summary>
	/// Implementations of this interface can a resource that need to have external state tracked for deletion.
	/// </summary>
	public interface IManagedResource
	{
		/// <summary>
		/// Set the time of the last state change.
		/// </summary>
		/// <param name="time"> of the last state change. </param>
		void TimeOfLastStateChange(long time);

		/// <summary>
		/// Get the time of the last state change.
		/// </summary>
		/// <returns> the time of the last state change. </returns>
		long TimeOfLastStateChange();

		/// <summary>
		/// Delete any resources held. This method should be idempotent.
		/// </summary>
		void Delete();
	}
}
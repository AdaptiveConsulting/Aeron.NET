using Adaptive.Aeron.Status;

namespace Adaptive.Aeron
{
	/// <summary>
	/// Interface for handling various error frame messages for publications.
	/// @since 1.47.0
	/// </summary>
	public interface IPublicationErrorFrameHandler
	{
		/// <summary>
		/// Called when an error frame for a publication is received by the local driver and needs to be propagated to the
		/// appropriate clients. E.g. when an image is invalidated. This callback will reuse the {@link
		/// PublicationErrorFrame} instance, so data is only valid for the lifetime of the callback. If the user needs to
		/// pass the data onto another thread or hold in another location for use later, then the user needs to make use of
		/// the <seealso cref="PublicationErrorFrame.Clone()"/> method to create a copy for their own use.
		/// <para>
		/// This callback will be executed on the client conductor thread, similar to image availability notifications.
		/// </para>
		/// <para>
		/// This notification will only be propagated to clients that have added an instance of the Publication that received
		/// the error frame (i.e. the originalRegistrationId matches the registrationId on the error frame).
		/// 
		/// </para>
		/// </summary>
		/// <param name="errorFrame"> containing the relevant information about the publication and the error message. </param>
		void OnPublicationError(PublicationErrorFrame errorFrame);
	}

	public class NoOpPublicationErrorFrameHandler : IPublicationErrorFrameHandler
	{
		public void OnPublicationError(PublicationErrorFrame errorFrame)
		{
			
		}
	}
}
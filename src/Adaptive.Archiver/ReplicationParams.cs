using Adaptive.Aeron.Security;

namespace Adaptive.Archiver
{
	/// <summary>
	/// Contains the optional parameters that can be passed to a Replication Request. Controls the behaviour of the
	/// replication including tagging, stop position, extending destination recordings, live merging, and setting the
	/// maximum length of the file I/O operations.
	/// </summary>
	public class ReplicationParams
	{
		private long stopPosition;
		private long dstRecordingId;
		private string liveDestination;
		private string replicationChannel;
		private long channelTagId;
		private long subscriptionTagId;
		private int fileIoMaxLength;
		private int replicationSessionId;
		private byte[] encodedCredentials;

		/// <summary>
		/// Initialise all parameters to defaults.
		/// </summary>
		public ReplicationParams()
		{
			Reset();
		}

		/// <summary>
		/// Reset the state of the parameters to the default for reuse.
		/// </summary>
		/// <returns> this for a fluent API. </returns>
		public ReplicationParams Reset()
		{
			stopPosition = AeronArchive.NULL_POSITION;
			dstRecordingId = Aeron.Aeron.NULL_VALUE;
			liveDestination = null;
			replicationChannel = null;
			channelTagId = Aeron.Aeron.NULL_VALUE;
			subscriptionTagId = Aeron.Aeron.NULL_VALUE;
			fileIoMaxLength = Aeron.Aeron.NULL_VALUE;
			replicationSessionId = Aeron.Aeron.NULL_VALUE;
			encodedCredentials = NullCredentialsSupplier.NULL_CREDENTIAL;
			return this;
		}

		/// <summary>
		/// Set the stop position for replication, default is <seealso cref="AeronArchive.NULL_POSITION"/>, which will continuously
		/// replicate.
		/// </summary>
		/// <param name="stopPosition"> position to stop the replication at. </param>
		/// <returns> this for a fluent API </returns>
		public ReplicationParams StopPosition(long stopPosition)
		{
			this.stopPosition = stopPosition;
			return this;
		}

		/// <summary>
		/// The stop position for this replication request. </summary>
		/// <returns> stop position </returns>
		public long StopPosition()
		{
			return stopPosition;
		}

		/// <summary>
		/// The recording in the local archive to extend. Default is <seealso cref="Aeron.Aeron.NULL_VALUE"/> which will trigger the creation
		/// of a new recording in the destination archive.
		/// </summary>
		/// <param name="dstRecordingId"> destination recording to extend. </param>
		/// <returns> this for a fluent API. </returns>
		public ReplicationParams DstRecordingId(long dstRecordingId)
		{
			this.dstRecordingId = dstRecordingId;
			return this;
		}

		/// <summary>
		/// Destination recording id to extend.
		/// </summary>
		/// <returns> destination recording id. </returns>
		public long DstRecordingId()
		{
			return dstRecordingId;
		}

		/// <summary>
		/// Destination for the live stream if merge is required. Default is null for no merge.
		/// </summary>
		/// <param name="liveChannel"> for the live stream merge </param>
		/// <returns> this for a fluent API. </returns>
		public ReplicationParams LiveDestination(string liveChannel)
		{
			this.liveDestination = liveChannel;
			return this;
		}

		/// <summary>
		/// Gets the destination for the live stream merge.
		/// </summary>
		/// <returns> destination for live stream merge. </returns>
		public string LiveDestination()
		{
			return liveDestination;
		}

		/// <summary>
		/// Channel to use for replicating the recording, empty string will mean that the default channel is used. </summary>
		/// <returns> channel to replicate the recording. </returns>
		public string ReplicationChannel()
		{
			return replicationChannel;
		}

		/// <summary>
		/// Channel use to replicate the recording. Default is null which will use the context's default replication
		/// channel
		/// </summary>
		/// <param name="replicationChannel"> to use for replicating the recording. </param>
		/// <returns> this for a fluent API. </returns>
		public ReplicationParams ReplicationChannel(string replicationChannel)
		{
			this.replicationChannel = replicationChannel;
			return this;
		}

		/// <summary>
		/// The channel used by the archive's subscription for replication will have the supplied channel tag applied to it.
		/// The default value for channelTagId is <seealso cref="Aeron.Aeron.NULL_VALUE"/>
		/// </summary>
		/// <param name="channelTagId"> tag to apply to the archive's subscription. </param>
		/// <returns> this for a fluent API </returns>
		public ReplicationParams ChannelTagId(long channelTagId)
		{
			this.channelTagId = channelTagId;
			return this;
		}

		/// <summary>
		/// Gets channel tag id for the archive subscription.
		/// </summary>
		/// <returns> channel tag id. </returns>
		public long ChannelTagId()
		{
			return channelTagId;
		}

		/// <summary>
		/// The channel used by the archive's subscription for replication will have the supplied subscription tag applied to
		/// it. The default value for subscriptionTagId is <seealso cref="Aeron.Aeron.NULL_VALUE"/>
		/// </summary>
		/// <param name="subscriptionTagId"> tag to apply to the archive's subscription. </param>
		/// <returns> this for a fluent API </returns>
		public ReplicationParams SubscriptionTagId(long subscriptionTagId)
		{
			this.subscriptionTagId = subscriptionTagId;
			return this;
		}

		/// <summary>
		/// Gets subscription tag id for the archive subscription.
		/// </summary>
		/// <returns> subscription tag id. </returns>
		public long SubscriptionTagId()
		{
			return subscriptionTagId;
		}

		/// <summary>
		/// The maximum size of a file operation when reading from the archive to execute the replication. Will use the value
		/// defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
		/// priority of some replays. Setting it to a value larger than the context value will have no affect.
		/// </summary>
		/// <param name="fileIoMaxLength"> maximum length of a file I/O operation. </param>
		/// <returns> this for a fluent API </returns>
		public ReplicationParams FileIoMaxLength(int fileIoMaxLength)
		{
			this.fileIoMaxLength = fileIoMaxLength;
			return this;
		}

		/// <summary>
		/// Gets the maximum length for file IO operations in the replay. Defaults to <seealso cref="Aeron.Aeron.NULL_VALUE"/> if not
		/// set, which will trigger the use of the Archive.Context default.
		/// </summary>
		/// <returns> maximum length of a file I/O operation. </returns>
		public int FileIoMaxLength()
		{
			return this.fileIoMaxLength;
		}

		/// <summary>
		/// Sets the session-id to be used for the replicated file instead of the session id from the source archive. This
		/// is useful in cases where we are replicating the same recording in multiple stages.
		/// </summary>
		/// <param name="replicationSessionId"> the session-id to be set for the received recording. </param>
		/// <returns> this for fluent API </returns>
		public ReplicationParams ReplicationSessionId(int replicationSessionId)
		{
			this.replicationSessionId = replicationSessionId;
			return this;
		}

		/// <summary>
		/// The session-id to be used for the replicated recording.
		/// </summary>
		/// <returns> session-id to be useful for the replicated recording. </returns>
		public int ReplicationSessionId()
		{
			return this.replicationSessionId;
		}

		/// <summary>
		/// Sets the encoded credentials that will be passed to the source archive for authentication. Currently only simple
		/// authentication (i.e. not challenge/response) is supported for replication.
		/// </summary>
		/// <param name="encodedCredentials"> credentials to be passed to the source archive. </param>
		/// <returns> this for a fluent API. </returns>
		public ReplicationParams EncodedCredentials(byte[] encodedCredentials)
		{
			this.encodedCredentials = encodedCredentials;
			return this;
		}

		/// <summary>
		/// Gets the encoded credentials that will be used to authenticate against the source archive.
		/// </summary>
		/// <returns> encoded credentials used for authentication. </returns>
		public byte[] EncodedCredentials()
		{
			return encodedCredentials;
		}

		public override string ToString()
		{
			return "ReplicationParams{" +
			       "stopPosition=" + stopPosition +
			       ", dstRecordingId=" + dstRecordingId +
			       ", liveDestination='" + liveDestination + '\'' +
			       ", replicationChannel='" + replicationChannel + '\'' +
			       ", channelTagId=" + channelTagId +
			       ", subscriptionTagId=" + subscriptionTagId +
			       ", fileIoMaxLength=" + fileIoMaxLength +
			       ", replicationSessionId=" + replicationSessionId +
			       '}';
		}

		private bool Equals(ReplicationParams other)
		{
			return stopPosition == other.stopPosition && dstRecordingId == other.dstRecordingId && 
			       liveDestination == other.liveDestination && replicationChannel == other.replicationChannel && 
			       channelTagId == other.channelTagId && subscriptionTagId == other.subscriptionTagId && 
			       fileIoMaxLength == other.fileIoMaxLength && replicationSessionId == other.replicationSessionId 
			       && Equals(encodedCredentials, other.encodedCredentials);
		}

		public override bool Equals(object obj)
		{
			if (obj is null) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((ReplicationParams)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = stopPosition.GetHashCode();
				hashCode = (hashCode * 397) ^ dstRecordingId.GetHashCode();
				hashCode = (hashCode * 397) ^ (liveDestination != null ? liveDestination.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (replicationChannel != null ? replicationChannel.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ channelTagId.GetHashCode();
				hashCode = (hashCode * 397) ^ subscriptionTagId.GetHashCode();
				hashCode = (hashCode * 397) ^ fileIoMaxLength;
				hashCode = (hashCode * 397) ^ replicationSessionId;
				hashCode = (hashCode * 397) ^ (encodedCredentials != null ? encodedCredentials.GetHashCode() : 0);
				return hashCode;
			}
		}
	}
}
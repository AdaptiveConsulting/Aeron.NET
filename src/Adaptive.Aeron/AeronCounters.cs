using System;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.Status;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
	/// <summary>
	/// This class serves as a registry for all counter type IDs used by Aeron.
	/// <para>
	/// The following ranges are reserved:
	/// <ul>
	///     <li>{@code 0 - 99}: for client/driver counters.</li>
	///     <li>{@code 100 - 199}: for archive counters.</li>
	///     <li>{@code 200 - 299}: for cluster counters.</li>
	/// </ul>
	/// </para>
	/// </summary>
	public static class AeronCounters
	{
		// Client/driver counters

		/// <summary>
		/// System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
		/// </summary>
		public const int DRIVER_SYSTEM_COUNTER_TYPE_ID = 0;

		/// <summary>
		/// The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will
		/// experience back pressure when this position is passed as a means of flow control.
		/// </summary>
		public const int DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1;

		/// <summary>
		/// The position the Sender has reached for sending data to the media on a session-channel-stream tuple.
		/// </summary>
		public const int DRIVER_SENDER_POSITION_TYPE_ID = 2;

		/// <summary>
		/// The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
		/// It is possible the stream is not complete to this point if the stream has experienced loss.
		/// </summary>
		public const int DRIVER_RECEIVER_HWM_TYPE_ID = 3;

		/// <summary>
		/// The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have
		/// multiple
		/// </summary>
		public const int DRIVER_SUBSCRIBER_POSITION_TYPE_ID = 4;

		/// <summary>
		/// The highest position the Receiver has rebuilt up to on a session-channel-stream tuple while rebuilding the
		/// stream.
		/// The stream is complete up to this point.
		/// </summary>
		public const int DRIVER_RECEIVER_POS_TYPE_ID = 5;

		/// <summary>
		/// The status of a send-channel-endpoint represented as a counter value.
		/// </summary>
		public const int DRIVER_SEND_CHANNEL_STATUS_TYPE_ID = 6;

		/// <summary>
		/// The status of a receive-channel-endpoint represented as a counter value.
		/// </summary>
		public const int DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

		/// <summary>
		/// The position the Sender can immediately send up-to on a session-channel-stream tuple.
		/// </summary>
		public const int DRIVER_SENDER_LIMIT_TYPE_ID = 9;

		/// <summary>
		/// A counter per Image indicating presence of the congestion control.
		/// </summary>
		public const int DRIVER_PER_IMAGE_TYPE_ID = 10;

		/// <summary>
		/// A counter for tracking the last heartbeat of an entity with a given registration id.
		/// </summary>
		public const int DRIVER_HEARTBEAT_TYPE_ID = 11;

		/// <summary>
		/// The position in bytes a publication has reached appending to the log.
		/// <para>
		/// <b>Note:</b> This is a not a real-time value like the other and is updated one per second for monitoring
		/// purposes.
		/// </para>
		/// </summary>
		public const int DRIVER_PUBLISHER_POS_TYPE_ID = 12;

		/// <summary>
		/// Count of back-pressure events (BPE)s a sender has experienced on a stream.
		/// </summary>
		public const int DRIVER_SENDER_BPE_TYPE_ID = 13;

		/// <summary>
		/// Count of media driver neighbors for name resolution.
		/// </summary>
		public const int NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 15;

		/// <summary>
		/// Count of entries in the name resolver cache.
		/// </summary>
		public const int NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 16;

		/// <summary>
		/// Counter used to store the status of a bind address and port for the local end of a channel.
		/// <para>
		/// When the value is <seealso cref="ChannelEndpointStatus.ACTIVE"/> then the key value and label will be updated with the
		/// socket address and port which is bound.
		/// </para>
		/// </summary>
		public const int DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

		/// <summary>
		/// Count of number of active receivers for flow control strategy.
		/// </summary>
		public const int FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID = 17;

		/// <summary>
		/// Count of number of destinations for multi-destination cast channels. 
		/// </summary>
		public const int MDC_DESTINATIONS_COUNTER_TYPE_ID = 18;


		// Archive counters
		/// <summary>
		/// The position a recording has reached when being archived.
		/// </summary>
		public const int ARCHIVE_RECORDING_POSITION_TYPE_ID = 100;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the number of errors that have occurred.
		/// </summary>
		public const int ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the count of concurrent control sessions.
		/// </summary>
		public const int ARCHIVE_CONTROL_SESSIONS_TYPE_ID = 102;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the max duty cycle time of an archive agent.
		/// </summary>
		public const int ARCHIVE_MAX_CYCLE_TIME_TYPE_ID = 103;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the count of cycle time threshold exceeded of
		/// an archive agent.
		/// </summary>
		public const int ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 104;

		// Cluster counters

		/// <summary>
		/// Counter type id for the consensus module state.
		/// </summary>
		public const int CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;

		/// <summary>
		/// Counter type id for the cluster node role.
		/// </summary>
		public const int CLUSTER_NODE_ROLE_TYPE_ID = 201;

		/// <summary>
		/// Counter type id for the control toggle.
		/// </summary>
		public const int CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;

		/// <summary>
		/// Counter type id of the commit position.
		/// </summary>
		public const int CLUSTER_COMMIT_POSITION_TYPE_ID = 203;

		/// <summary>
		/// Counter representing the Recovery State for the cluster.
		/// </summary>
		public const int CLUSTER_RECOVERY_STATE_TYPE_ID = 204;

		/// <summary>
		/// Counter type id for count of snapshots taken.
		/// </summary>
		public const int CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;

		/// <summary>
		/// Type id for election state counter.
		/// </summary>
		public const int CLUSTER_ELECTION_STATE_TYPE_ID = 207;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for the backup state.
		/// </summary>
		public const int CLUSTER_BACKUP_STATE_TYPE_ID = 208;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for the live log position counter.
		/// </summary>
		public const int CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for the next query deadline counter.
		/// </summary>
		public const int CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the number of errors that have occurred.
		/// </summary>
		public const int CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;

		/// <summary>
		/// Counter type id for the consensus module error count.
		/// </summary>
		public const int CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

		/// <summary>
		/// Counter type id for the number of cluster clients which have been timed out.
		/// </summary>
		public const int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

		/// <summary>
		/// Counter type id for the number of invalid requests which the cluster has received.
		/// </summary>
		public const int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

		/// <summary>
		/// Counter type id for the clustered service error count.
		/// </summary>
		public const int CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the max duty cycle time of the consensus module.
		/// </summary>
		public const int CLUSTER_MAX_CYCLE_TIME_TYPE_ID = 216;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the count of cycle time threshold exceeded of
		/// the consensus module.
		/// </summary>
		public const int CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 217;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the max duty cycle time of the service container.
		/// </summary>
		public const int CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID = 218;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for keeping track of the count of cycle time threshold exceeded of
		/// the service container.
		/// </summary>
		public const int CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 219;

		/// <summary>
		/// The type id of the <seealso cref="Counter"/> used for the warm standby state.
		/// </summary>
		public const int CLUSTER_STANDBY_STATE_TYPE_ID = 220;

		/// <summary>
		/// Counter type id for the clustered service error count.
		/// </summary>
		public const int CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID = 221;

		/// <summary>
		/// Counter type for responses to heartbeat request from the cluster.
		/// </summary>
		public const int CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID = 222;

		/// <summary>
		/// Checks that the counter specified by {@code counterId} has the counterTypeId that matches the specified value.
		/// If not it will throw a <seealso cref="ConfigurationException"/>.
		/// </summary>
		/// <param name="countersReader"> to look up the counter type id. </param>
		/// <param name="counterId"> counter to reference. </param>
		/// <param name="expectedCounterTypeId"> the expected type id for the counter. </param>
		/// <exception cref="ConfigurationException"> if the type id does not match. </exception>
		/// <exception cref="ArgumentException"> if the counterId is not valid. </exception>
		public static void ValidateCounterTypeId(CountersReader countersReader, int counterId,
			int expectedCounterTypeId)
		{
			int counterTypeId = countersReader.GetCounterTypeId(counterId);
			if (expectedCounterTypeId != counterTypeId)
			{
				throw new ConfigurationException("The type for counterId=" + counterId + ", typeId=" + counterTypeId +
				                                 " does not match the expected=" + expectedCounterTypeId);
			}
		}

		/// <summary>
		/// Convenience overload for <seealso cref="AeronCounters.ValidateCounterTypeId(CountersReader, int, int)"/>
		/// </summary>
		/// <param name="aeron"> to resolve a counters' reader. </param>
		/// <param name="counter"> to be checked for the appropriate counterTypeId. </param>
		/// <param name="expectedCounterTypeId"> the expected type id for the counter. </param>
		/// <exception cref="ConfigurationException"> if the type id does not match. </exception>
		/// <exception cref="ArgumentException"> if the counterId is not valid. </exception>
		/// <seealso cref="AeronCounters.ValidateCounterTypeId(CountersReader, int, int)"/>
		public static void ValidateCounterTypeId(Aeron aeron, Counter counter, int expectedCounterTypeId)
		{
			ValidateCounterTypeId(aeron.CountersReader, counter.Id, expectedCounterTypeId);
		}
	}
}
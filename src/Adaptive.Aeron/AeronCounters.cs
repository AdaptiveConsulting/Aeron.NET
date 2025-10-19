using System;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Agrona.BitUtil;
using static Adaptive.Agrona.Concurrent.Status.CountersReader;

namespace Adaptive.Aeron
{
    /// <summary>
    /// This class serves as a registry for all counter type IDs used by Aeron.
    /// <para>
    /// Type IDs less than 1000 are reserved for Aeron use. Any custom counters should use a typeId of 1000 or higher.
    /// Aeron uses the following specific ranges:
    /// <ul>
    ///     <li>{@code 0 - 99}: for client/driver counters.</li>
    ///     <li>{@code 100 - 199}: for archive counters.</li>
    ///     <li>{@code 200 - 299}: for cluster counters.</li>
    /// </ul>
    /// </para>
    /// </summary>
    public static class AeronCounters
    {
        // System counter IDs to be accessed outside the driver.
        /// <summary>
        /// Counter id for bytes sent over the network.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_BYTES_SENT = 0;

        /// <summary>
        /// Counter id for bytes sent over the network.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_BYTES_RECEIVED = 1;

        /// <summary>
        /// Counter id for failed offers to the receiver proxy.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS = 2;

        /// <summary>
        /// Counter id for failed offers to the sender proxy.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS = 3;

        /// <summary>
        /// Counter id for failed offers to the conductor proxy.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_CONDUCTOR_PROXY_FAILS = 4;

        /// <summary>
        /// Counter id for NAKs sent back to senders requesting re-transmits.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT = 5;

        /// <summary>
        /// Counter id for NAKs received from receivers requesting re-transmits.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED = 6;

        /// <summary>
        /// Counter id for status messages sent back to senders for flow control.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT = 7;

        /// <summary>
        /// Counter id for status messages received from receivers for flow control.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED = 8;

        /// <summary>
        /// Counter id for heartbeat data frames sent to indicate liveness in the absence of data to send.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_HEARTBEATS_SENT = 9;

        /// <summary>
        /// Counter id for heartbeat data frames received to indicate liveness in the absence of data to send.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED = 10;

        /// <summary>
        /// Counter id for data packets re-transmitted as a result of NAKs.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RETRANSMITS_SENT = 11;

        /// <summary>
        /// Counter id for packets received which under-run the current flow control window for images.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS = 12;

        /// <summary>
        /// Counter id for packets received which over-run the current flow control window for images.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS = 13;

        /// <summary>
        /// Counter id for invalid packets received.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_INVALID_PACKETS = 14;

        /// <summary>
        /// Counter id for errors observed by the driver and an indication to read the distinct error log.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_ERRORS = 15;

        /// <summary>
        /// Counter id for socket send operation which resulted in less than the packet length being sent.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_SHORT_SENDS = 16;

        /// <summary>
        /// Counter id for attempts to free log buffers no longer required by the driver which as still held by clients.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_FREE_FAILS = 17;

        /// <summary>
        /// Counter id for the times a sender has entered the state of being back-pressured when it could have sent faster.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_SENDER_FLOW_CONTROL_LIMITS = 18;

        /// <summary>
        /// Counter id for the times a publication has been unblocked after a client failed to complete an offer within a
        /// timeout.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_UNBLOCKED_PUBLICATIONS = 19;

        /// <summary>
        /// Counter id for the times a command has been unblocked after a client failed to complete an offer within a
        /// timeout.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS = 20;

        /// <summary>
        /// Counter id for the times the channel endpoint detected a possible TTL asymmetry between its config and new
        /// connection.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_POSSIBLE_TTL_ASYMMETRY = 21;

        /// <summary>
        /// Counter id for status of the <seealso cref="ControllableIdleStrategy"/> if configured.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_CONTROLLABLE_IDLE_STRATEGY = 22;

        /// <summary>
        /// Counter id for the times a loss gap has been filled when NAKs have been disabled.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_LOSS_GAP_FILLS = 23;

        /// <summary>
        /// Counter id for the Aeron clients that have timed out without a graceful close.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_CLIENT_TIMEOUTS = 24;

        /// <summary>
        /// Counter id for the times a connection endpoint has been re-resolved resulting in a change.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RESOLUTION_CHANGES = 25;

        /// <summary>
        /// Counter id for the maximum time spent by the conductor between work cycles.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME = 26;

        /// <summary>
        /// Counter id for the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED = 27;

        /// <summary>
        /// Counter id for the maximum time spent by the sender between work cycles.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME = 28;

        /// <summary>
        /// Counter id for the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED = 29;

        /// <summary>
        /// Counter id for the maximum time spent by the receiver between work cycles.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME = 30;

        /// <summary>
        /// Counter id for the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED = 31;

        /// <summary>
        /// Counter id for the maximum time spent by the NameResolver in one of its operations.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_NAME_RESOLVER_MAX_TIME = 32;

        /// <summary>
        /// Counter id for the number of times the time threshold has been exceeded by the NameResolver.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED = 33;

        /// <summary>
        /// Counter id for the version of the media driver.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_AERON_VERSION = 34;

        /// <summary>
        /// Counter id for the total number of bytes currently mapped in log buffers, CnC file, and loss report.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_BYTES_CURRENTLY_MAPPED = 35;

        /// <summary>
        /// Counter id for the minimum bound on the number of bytes re-transmitted as a result of NAKs.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES = 36;

        /// <summary>
        /// Counter id for the number of times that the retransmit pool has been overflowed.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_RETRANSMIT_OVERFLOW = 37;

        /// <summary>
        /// Counter id for the number of error frames received by this driver.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_ERROR_FRAMES_RECEIVED = 38;

        /// <summary>
        /// Counter id for the number of error frames sent by this driver.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_ERROR_FRAMES_SENT = 39;

        /// <summary>
        /// Counter id for the number of publications that have been revoked.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED = 40;

        /// <summary>
        /// Counter id for the number of publication images that have been revoked.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED = 41;

        /// <summary>
        /// Counter id for the number of images that have been rejected.
        /// </summary>
        public const int SYSTEM_COUNTER_ID_IMAGES_REJECTED = 42;

        /// <summary>
        /// Counter id for the control protocol between clients and media driver.
        /// </summary>
        /// <remarks>Since 1.49.0</remarks> 
        public const int SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION = 43;

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
        /// stream. The stream is complete up to this point.
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

        /// <summary>
        /// The number of NAK messages received by the Sender.
        /// </summary>
        public const int DRIVER_SENDER_NAKS_RECEIVED_TYPE_ID = 19;

        /// <summary>
        /// The number of NAK messages sent by the Receiver.
        /// </summary>
        public const int DRIVER_RECEIVER_NAKS_SENT_TYPE_ID = 20;

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

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the max time it took recorder to write a block of
        /// data to the storage.
        /// </summary>
        public const int ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID = 105;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the total number of bytes written by the recorder
        /// to the storage.
        /// </summary>
        public const int ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID = 106;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the total time the recorder spent writing data to
        /// the storage.
        /// </summary>
        public const int ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID = 107;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the max time it took replayer to read a block from
        /// the storage.
        /// </summary>
        public const int ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID = 108;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the total number of bytes read by the replayer from
        /// the storage.
        /// </summary>
        public const int ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID = 109;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the total time the replayer spent reading data from
        /// the storage.
        /// </summary>
        public const int ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID = 110;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for tracking the count of active recording sessions.
        /// </summary>
        public const int ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID = 111;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for tracking the count of active replay sessions.
        /// </summary>
        public const int ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID = 112;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for tracking Archive clients.
        /// </summary>
        /// <remarks>Since 1.49.0</remarks>
        public const int ARCHIVE_CONTROL_SESSION_TYPE_ID = 113;

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
        /// Standby control toggle type id.
        /// </summary>
        public const int CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID = 223;

        /// <summary>
        /// The type if of the <seealso cref="Counter"/> used for transition module state.
        /// </summary>
        public const int TRANSITION_MODULE_STATE_TYPE_ID = 224;

        /// <summary>
        /// Transition module control toggle type id.
        /// </summary>
        public const int TRANSITION_MODULE_CONTROL_TOGGLE_TYPE_ID = 225;

        /// <summary>
        /// Counter type id for the transition module error count.
        /// </summary>
        public const int TRANSITION_MODULE_ERROR_COUNT_TYPE_ID = 226;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the max duty cycle time of the cluster standby.
        /// </summary>
        public const int CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID = 227;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the count of cycle time threshold exceeded of
        /// the cluster standby.
        /// </summary>
        public const int CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 228;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the max duty cycle time of the transition module.
        /// </summary>
        public const int TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID = 229;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the count of cycle time threshold exceeded of
        /// the transition module.
        /// </summary>
        public const int TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 230;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> to make visible the memberId that the cluster standby is currently using to
        /// as a source for the cluster log.
        /// </summary>
        public const int CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID = 231;

        /// <summary>
        /// Counter type for count of standby snapshots received.
        /// </summary>
        public const int CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID = 232;

        /// <summary>
        /// The type of the <seealso cref="Counter"/> used for handling node specific operations.
        /// </summary>
        public const int NODE_CONTROL_TOGGLE_TYPE_ID = 233;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the maximum total snapshot duration.
        /// </summary>
        public const int CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID = 234;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the count total snapshot duration
        /// has exceeded the threshold.
        /// </summary>
        public const int CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 235;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the maximum snapshot duration
        /// for a given clustered service.
        /// </summary>
        public const int CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID = 236;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the count snapshot duration
        /// has exceeded the threshold for a given clustered service.
        /// </summary>
        public const int CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 237;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the number of elections that have occurred.
        /// </summary>
        public const int CLUSTER_ELECTION_COUNT_TYPE_ID = 238;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for keeping track of the Cluster leadership term id.
        /// </summary>
        public const int CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID = 239;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for tracking the number of snapshots downloaded.
        /// </summary>
        public const int CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID = 240;

        /// <summary>
        /// The type id of the <seealso cref="Counter"/> used for tracking Cluster clients.
        /// </summary>
        /// <remarks>Since 1.49.0</remarks>
        public const int CLUSTER_SESSION_TYPE_ID = 241;

        // ===================
        // Sequencer Counters.
        // ===================

        /// <summary>
        /// Counter id for Sequencer Index.
        /// </summary>
        public const int SEQUENCER_INDEX_COUNTER_TYPE_ID = 500;

        /// <summary>
        /// Counter id for application group last message.
        /// </summary>
        public const int SEQUENCER_GROUP_HWM_COUNTER_TYPE_ID = 501;

        /// <summary>
        /// Counter id for session last message.
        /// </summary>
        public const int SEQUENCER_SESSION_GREATEST_MESSAGE_ID_COUNTER_TYPE_ID = 502;

        /// <summary>
        /// Counter id for session messages.
        /// </summary>
        public const int SEQUENCER_SESSION_MESSAGES_COUNTER_TYPE_ID = 503;

        /// <summary>
        /// Counter id for session last message timestamp.
        /// </summary>
        public const int SEQUENCER_SESSION_GREATEST_MESSAGE_TIMESTAMP_COUNTER_TYPE_ID = 504;

        /// <summary>
        /// Counter id for the next snapshot id.
        /// </summary>
        public const int SEQUENCER_CLIENT_SNAPSHOT_ID_COUNTER_TYPE_ID = 505;

        /// <summary>
        /// Counter id for sequence index.
        /// </summary>
        public const int SEQUENCER_APPLICATION_SEQUENCE_INDEX_COUNTER_TYPE_ID = 507;

        /// <summary>
        /// Application state counter type id.
        /// </summary>
        public const int SEQUENCER_APPLICATION_STATE_COUNTER_TYPE_ID = 508;

        /// <summary>
        /// Counter id for error count.
        /// </summary>
        public const int SEQUENCER_APPLICATION_ERROR_COUNT_TYPE_ID = 509;

        /// <summary>
        /// Counter id for max service time.
        /// </summary>
        public const int SEQUENCER_APPLICATION_MAX_SERVICE_TIME_TYPE_ID = 510;

        /// <summary>
        /// Counter id for the number of times the service time threshold was exceeded.
        /// </summary>
        public const int SEQUENCER_APPLICATION_SERVICE_TIME_THRESHOLD_EXCEEDED_COUNT_TYPE_ID = 511;

        /// <summary>
        /// Counter id for the total service time during the last interval.
        /// </summary>
        public const int SEQUENCER_APPLICATION_INTERVAL_SERVICE_TIME_TYPE_ID = 512;

        /// <summary>
        /// Counter id for the maximum individual service time during the last interval.
        /// </summary>
        public const int SEQUENCER_APPLICATION_INTERVAL_MAX_SERVICE_TIME_TYPE_ID = 513;

        /// <summary>
        /// Counter id for the total number of invocations during the last interval.
        /// </summary>
        public const int SEQUENCER_APPLICATION_INTERVAL_TOTAL_INVOCATIONS_TYPE_ID = 514;

        /// <summary>
        /// Counter id for the load time, in milliseconds, of a snapshot.
        /// </summary>
        public const int SEQUENCER_APPLICATION_SNAPSHOT_LOAD_TIME_TYPE_ID = 515;

        /// <summary>
        /// Counter id for the store time, in milliseconds, of a snapshot.
        /// </summary>
        public const int SEQUENCER_APPLICATION_SNAPSHOT_STORE_TIME_TYPE_ID = 516;

        /// <summary>
        /// Counter id for the number of 'take snapshot' failures.
        /// </summary>
        public const int SEQUENCER_APPLICATION_TAKE_SNAPSHOT_FAILURES_TYPE_ID = 517;

        /// <summary>
        /// Counter id for the number of 'take snapshot' instances.
        /// </summary>
        public const int SEQUENCER_APPLICATION_TAKE_SNAPSHOT_COUNT_TYPE_ID = 518;

        /// <summary>
        /// Counter id for the application service's session with the sequencer.
        /// </summary>
        public const int SEQUENCER_APPLICATION_SESSION_ID_TYPE_ID = 519;

        /// <summary>
        /// Counter id for the replay index's minimum sequence index.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_MIN_SEQUENCE_INDEX_COUNTER_TYPE_ID = 520;

        /// <summary>
        /// Counter id for the replay index's minimum log position.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_MIN_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 521;

        /// <summary>
        /// Counter id for the replay index's maximum sequence index.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_MAX_SEQUENCE_INDEX_COUNTER_TYPE_ID = 522;

        /// <summary>
        /// Counter id for the replay index's maximum log position.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_MAX_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 523;

        /// <summary>
        /// Counter id for the replay index's initial sequence index.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_INITIAL_SEQUENCE_INDEX_COUNTER_TYPE_ID = 524;

        /// <summary>
        /// Counter id for the replay index's initial log position.
        /// </summary>
        public const int SEQUENCER_REPLAY_INDEX_INITIAL_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 525;

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
                throw new ConfigurationException("The type for counterId=" + counterId +
                                                 ", typeId=" + counterTypeId +
                                                 " does not match the expected=" + expectedCounterTypeId);
            }
        }

        /// <summary>
        /// Convenience overload for <seealso cref="AeronCounters.ValidateCounterTypeId(CountersReader, int, int)"/>.
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

        /// <summary>
        /// Append version information at the end of the counter's label.
        /// </summary>
        /// <param name="tempBuffer">     to append label to. </param>
        /// <param name="offset">         at which current label data ends. </param>
        /// <param name="fullVersion">    of the component. </param>
        /// <returns> length of the suffix appended. </returns>
        public static int AppendVersionInfo(IMutableDirectBuffer tempBuffer, int offset, string fullVersion)
        {
            int length = tempBuffer.PutStringWithoutLengthAscii(offset, " ");
            length += tempBuffer.PutStringWithoutLengthAscii(
                offset + length, FormatVersionInfo(fullVersion));
            return length;
        }

        /// <summary>
        /// Append specified {@code value} at the end of the counter's label as ASCII encoded value up to the
        /// <seealso cref="CountersReader.MAX_LABEL_LENGTH"/>.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the counter metadata. </param>
        /// <param name="counterId">      to append version info to. </param>
        /// <param name="value">          to be appended to the label. </param>
        /// <returns> number of bytes that got appended. </returns>
        /// <exception cref="ArgumentException"> if {@code counterId} is invalid or points to non-allocated counter. </exception>
        public static int AppendToLabel(IAtomicBuffer metaDataBuffer, int counterId, string value)
        {
            if (null == metaDataBuffer) throw new ArgumentNullException(nameof(metaDataBuffer));
            ValidateCounterId(metaDataBuffer, counterId);

            int counterMetaDataOffset = MetaDataOffset(counterId);
            int state = metaDataBuffer.GetIntVolatile(counterMetaDataOffset);
            if (RECORD_ALLOCATED != state)
            {
                throw new ArgumentException("counter id " + counterId + " is not allocated, state: " + state);
            }

            int existingLabelLength = metaDataBuffer.GetInt(counterMetaDataOffset + LABEL_OFFSET);
            int remainingLabelLength = MAX_LABEL_LENGTH - existingLabelLength;

            int writtenLength = metaDataBuffer.PutStringWithoutLengthAscii(
                counterMetaDataOffset + LABEL_OFFSET + SIZE_OF_INT + existingLabelLength,
                value,
                0,
                remainingLabelLength);
            if (writtenLength > 0)
            {
                metaDataBuffer.PutIntRelease(
                    counterMetaDataOffset + LABEL_OFFSET, existingLabelLength + writtenLength);
            }

            return writtenLength;
        }

        /// <summary>
        /// Format version information for display purposes.
        /// </summary>
        /// <param name="fullVersion"> of the component. </param>
        /// <returns> formatted String. </returns>
        public static string FormatVersionInfo(string fullVersion)
        {
            return "version=" + fullVersion;
        }

        private static void ValidateCounterId(IAtomicBuffer metaDataBuffer, int counterId)
        {
            if (counterId < 0)
            {
                throw new ArgumentException("counter id " + counterId + " is negative");
            }

            int maxCounterId = (metaDataBuffer.Capacity / METADATA_LENGTH) - 1;
            if (counterId > maxCounterId)
            {
                throw new ArgumentException(
                    "counter id " + counterId + " out of range: 0 - maxCounterId=" + maxCounterId);
            }
        }
    }
}
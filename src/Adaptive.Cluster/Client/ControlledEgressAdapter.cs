using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using static Adaptive.Cluster.Client.AeronCluster;

namespace Adaptive.Cluster.Client
{
	/// <summary>
	/// Adapter for dispatching egress messages from a cluster to a <seealso cref="IControlledEgressListener"/>.
	/// </summary>
	public sealed class ControlledEgressAdapter : IControlledFragmentHandler
	{
		private readonly long clusterSessionId;
		private readonly int fragmentLimit;
		private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
		private readonly SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
		private readonly NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
		private readonly AdminResponseDecoder adminResponseDecoder = new AdminResponseDecoder();
		private readonly SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
		private ControlledFragmentAssembler fragmentAssembler;
		private readonly IControlledEgressListener listener;
		private readonly IControlledEgressListenerExtension listenerExtension;
		private readonly Subscription subscription;

		/// <summary>
		/// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
		/// <seealso cref="IControlledEgressListener"/>.
		/// </summary>
		/// <param name="listener">         to dispatch events to. </param>
		/// <param name="clusterSessionId"> for the egress. </param>
		/// <param name="subscription">     over the egress stream. </param>
		/// <param name="fragmentLimit">    to poll on each <seealso cref="Poll()"/> operation. </param>
		public ControlledEgressAdapter(IControlledEgressListener listener, long clusterSessionId,
			Subscription subscription, int fragmentLimit) : this(listener, null, clusterSessionId, subscription,
			fragmentLimit)
		{
			fragmentAssembler = new ControlledFragmentAssembler(this);
		}

		/// <summary>
		/// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
		/// <seealso cref="IControlledEgressListener"/> or extension messages to <seealso cref="IControlledEgressListenerExtension"/>.
		/// </summary>
		/// <param name="listener">          to dispatch events to. </param>
		/// <param name="listenerExtension"> to dispatch extension messages to </param>
		/// <param name="clusterSessionId">  for the egress. </param>
		/// <param name="subscription">      over the egress stream. </param>
		/// <param name="fragmentLimit">     to poll on each <seealso cref="Poll()"/> operation. </param>
		public ControlledEgressAdapter(IControlledEgressListener listener,
			IControlledEgressListenerExtension listenerExtension, long clusterSessionId, Subscription subscription,
			int fragmentLimit)
		{
			this.clusterSessionId = clusterSessionId;
			this.fragmentLimit = fragmentLimit;
			this.listener = listener;
			this.listenerExtension = listenerExtension;
			this.subscription = subscription;
		}

		/// <summary>
		/// Poll the egress subscription and dispatch assembled events to the <seealso cref="IControlledEgressListener"/>.
		/// </summary>
		/// <returns> the number of fragments consumed. </returns>
		public int Poll()
		{
			return subscription.ControlledPoll(fragmentAssembler, fragmentLimit);
		}

		/// <summary>
		/// {@inheritDoc}
		/// </summary>
		public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
		{
			messageHeaderDecoder.Wrap(buffer, offset);

			int templateId = messageHeaderDecoder.TemplateId();
			int schemaId = messageHeaderDecoder.SchemaId();
			if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
			{
				if (listenerExtension != null)
				{
					return listenerExtension.OnExtensionMessage(messageHeaderDecoder.BlockLength(), templateId,
						schemaId, messageHeaderDecoder.Version(), buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
						length - MessageHeaderDecoder.ENCODED_LENGTH);
				}

				throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
				                           schemaId);
			}

			switch (templateId)
			{
				case SessionMessageHeaderDecoder.TEMPLATE_ID:
				{
					sessionMessageHeaderDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
						messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

					long sessionId = sessionMessageHeaderDecoder.ClusterSessionId();
					if (sessionId == clusterSessionId)
					{
						return listener.OnMessage(sessionId, sessionMessageHeaderDecoder.Timestamp(), buffer,
							offset + SESSION_HEADER_LENGTH, length - SESSION_HEADER_LENGTH, header);
					}

					break;
				}

				case SessionEventDecoder.TEMPLATE_ID:
				{
					sessionEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
						messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

					long sessionId = sessionEventDecoder.ClusterSessionId();
					if (sessionId == clusterSessionId)
					{
						listener.OnSessionEvent(sessionEventDecoder.CorrelationId(), sessionId,
							sessionEventDecoder.LeadershipTermId(), sessionEventDecoder.LeaderMemberId(),
							sessionEventDecoder.Code(), sessionEventDecoder.Detail());
					}

					break;
				}

				case NewLeaderEventDecoder.TEMPLATE_ID:
				{
					newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
						messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

					long sessionId = newLeaderEventDecoder.ClusterSessionId();
					if (sessionId == clusterSessionId)
					{
						listener.OnNewLeader(sessionId, newLeaderEventDecoder.LeadershipTermId(),
							newLeaderEventDecoder.LeaderMemberId(), newLeaderEventDecoder.IngressEndpoints());
					}

					break;
				}

				case AdminResponseDecoder.TEMPLATE_ID:
				{
					adminResponseDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
						messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

					long sessionId = adminResponseDecoder.ClusterSessionId();
					if (sessionId == clusterSessionId)
					{
						long correlationId = adminResponseDecoder.CorrelationId();
						AdminRequestType requestType = adminResponseDecoder.RequestType();
						AdminResponseCode responseCode = adminResponseDecoder.ResponseCode();
						string message = adminResponseDecoder.Message();
						int payloadOffset = adminResponseDecoder.Offset() + AdminResponseDecoder.BLOCK_LENGTH +
						                    AdminResponseDecoder.MessageHeaderLength() + message.Length +
						                    AdminResponseDecoder.PayloadHeaderLength();
						int payloadLength = adminResponseDecoder.PayloadLength();
						listener.OnAdminResponse(sessionId, correlationId, requestType, responseCode, message, buffer,
							payloadOffset, payloadLength);
					}

					break;
				}

				default:
					break;
			}

			return ControlledFragmentHandlerAction.CONTINUE;
		}
	}
}
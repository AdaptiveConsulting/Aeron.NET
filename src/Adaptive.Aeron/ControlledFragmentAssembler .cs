using System;
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// A <seealso cref="IControlledFragmentHandler"/> that sits in a chain-of-responsibility pattern that reassembles fragmented messages
    /// so that the next handler in the chain only sees whole messages.
    /// <para>
    /// Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
    /// buffer for reassembly before delegation.
    /// </para>
    /// <para>
    /// The <seealso cref="Header"/> passed to the delegate on assembling a message will be that of the last fragment.
    /// </para>
    /// <para>
    /// Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
    /// When sessions go inactive see <seealso cref="IUnavailableImageHandler"/>, it is possible to free the buffer by calling
    /// <seealso cref="FreeSessionBuffer(int)"/>.
    /// </para>
    /// </summary>
    public class ControlledFragmentAssembler : IControlledFragmentHandler
    {
        private readonly IControlledFragmentHandler _delegate;
        private readonly IDictionary<int, BufferBuilder> _builderBySessionIdMap = new Dictionary<int, BufferBuilder>();
        private readonly Func<int, BufferBuilder> _builderFunc;

        /// <summary>
        /// Construct an adapter to reassemble message fragments and delegate on only whole messages.
        /// </summary>
        /// <param name="delegate"> onto which whole messages are forwarded. </param>
        public ControlledFragmentAssembler(IControlledFragmentHandler @delegate) : this(@delegate, BufferBuilder.INITIAL_CAPACITY)
        {
        }

        /// <summary>
        /// Construct an adapter to reassembly message fragments and delegate on only whole messages.
        /// </summary>
        /// <param name="delegate">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public ControlledFragmentAssembler(IControlledFragmentHandler @delegate, int initialBufferLength)
        {
            _delegate = @delegate;
            _builderFunc = (ignore) => new BufferBuilder(initialBufferLength);
        }

        /// <summary>
        /// The implementation of <seealso cref="IControlledFragmentHandler"/> that reassembles and forwards whole messages.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        public virtual ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            byte flags = header.Flags();

            var action = ControlledFragmentHandlerAction.CONTINUE;

            if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
            {
                action = _delegate.OnFragment(buffer, offset, length, header);
            }
            else
            {
                if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) == FrameDescriptor.BEGIN_FRAG_FLAG)
                {
                    BufferBuilder builder;
                    if (!_builderBySessionIdMap.TryGetValue(header.SessionId(), out builder))
                    {
                        builder = _builderFunc(header.SessionId());
                        _builderBySessionIdMap[header.SessionId()] = builder;
                    }
                    builder.Reset().Append(buffer, offset, length);
                }
                else
                {
                    BufferBuilder builder;
                    if (_builderBySessionIdMap.TryGetValue(header.SessionId(), out builder))
                    {
                        int limit = builder.Limit();
                        builder.Append(buffer, offset, length);

                        if ((flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
                        {
                            int msgLength = builder.Limit();
                            action = _delegate.OnFragment(builder.Buffer(), 0, msgLength, header);

                            if (ControlledFragmentHandlerAction.ABORT == action)
                            {
                                builder.Limit(limit);
                            }
                            else
                            {
                                builder.Reset();
                            }
                        }
                    }
                }
            }

            return action;
        }

        /// <summary>
        /// Free an existing session buffer to reduce memory pressure when an image goes inactive or no more
        /// large messages are expected.
        /// </summary>
        /// <param name="sessionId"> to have its buffer freed </param>
        /// <returns> true if a buffer has been freed otherwise false. </returns>
        public bool FreeSessionBuffer(int sessionId)
        {
            if (_builderBySessionIdMap.ContainsKey(sessionId))
            {
                _builderBySessionIdMap.Remove(sessionId);
                return true;
            }
            return false;
        }
    }
}
using System;
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// A <seealso cref="IFragmentHandler"/> that sits in a chain-of-responsibility pattern that reassembles fragmented messages
    /// so that the next handler in the chain only sees whole messages.
    /// <para>
    /// Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
    /// buffer for reassembly before delegation.
    /// </para>
    /// <para>
    /// The <seealso cref="Header"/> passed to the _delegate on assembling a message will be that of the last fragment.
    /// </para>
    /// <para>
    /// Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
    /// When sessions go inactive see <seealso cref="UnavailableImageHandler"/>, it is possible to free the buffer by calling
    /// <seealso cref="FreeSessionBuffer(int)"/>.
    /// </para>
    /// </summary>
    public class FragmentAssembler : IFragmentHandler
    {
        private readonly IFragmentHandler _delegate;
        private readonly Dictionary<int, BufferBuilder> _builderBySessionIdMap = new Dictionary<int, BufferBuilder>();
        private readonly Func<int, BufferBuilder> _builderFunc;

        /// <summary>
        /// Construct an adapter to reassemble message fragments and _delegate on only whole messages.
        /// </summary>
        /// <param name="fragmentHandler"> onto which whole messages are forwarded. </param>
        public FragmentAssembler(IFragmentHandler fragmentHandler) : this(fragmentHandler, BufferBuilder.INITIAL_CAPACITY)
        {
        }

        /// <summary>
        /// Construct an adapter to reassembly message fragments and _delegate on only whole messages.
        /// </summary>
        /// <param name="fragmentHandler">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public FragmentAssembler(IFragmentHandler fragmentHandler, int initialBufferLength)
        {
            _delegate = fragmentHandler;
            _builderFunc = (ignore) => new BufferBuilder(initialBufferLength);
        }

        /// <summary>
        /// The implementation of <seealso cref="FragmentHandler"/> that reassembles and forwards whole messages.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            byte flags = header.Flags();

            if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
            {
                _delegate.OnFragment(buffer, offset, length, header);
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
                    BufferBuilder builder = _builderBySessionIdMap[header.SessionId()];
                    if (null != builder && builder.Limit() != 0)
                    {
                        builder.Append(buffer, offset, length);

                        if ((flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
                        {
                            int msgLength = builder.Limit();
                            _delegate.OnFragment(builder.Buffer(), 0, msgLength, header);
                            builder.Reset();
                        }
                    }
                }
            }
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
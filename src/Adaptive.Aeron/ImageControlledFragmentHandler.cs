using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// A <seealso cref="IControlledFragmentHandler"/> that sits in a chain-of-responsibility pattern that reassembles fragmented
    /// messages so that the next handler in the chain only sees whole messages. This is for a single session on an
    /// <seealso cref="Image"/> and not for multiple session <seealso cref="Image"/>s in a <seealso cref="Subscription"/>.
    /// 
    /// Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
    /// buffer for reassembly before delegation.
    /// 
    /// The <seealso cref="Header"/> passed to the delegate on assembling a message will be that of the last fragment.
    /// </summary>
    /// <see cref="Image.ControlledPoll(IControlledFragmentHandler, int)"/>
    /// <see cref="Image.ControlledPeek(long, IControlledFragmentHandler, long)"/>
    public class ImageControlledFragmentAssembler : IControlledFragmentHandler
    {
        private readonly IControlledFragmentHandler _delegate;
        private readonly BufferBuilder _builder;

        /// <summary>
        /// Construct an adapter to reassembly message fragments and delegate on only whole messages.
        /// </summary>
        /// <param name="delegate">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public ImageControlledFragmentAssembler(IControlledFragmentHandler @delegate, int initialBufferLength = 0, bool isDirect = false)
        {
            _delegate = @delegate;
            _builder = new BufferBuilder(initialBufferLength);
        }

        /// <summary>
        /// Get the delegate unto which assembled messages are delegated.
        /// </summary>
        /// <returns>  the delegate unto which assembled messages are delegated. </returns>
        public IControlledFragmentHandler Delegate()
        {
            return _delegate;
        }

        /// <summary>
        /// Get the <see cref="BufferBuilder"/> for resetting this assembler.
        /// </summary>
        /// <returns> the <see cref="BufferBuilder"/> for resetting this assembler</returns>
        public BufferBuilder Builder()
        {
            return _builder;
        }

        /// <summary>
        /// The implementation of <seealso cref="IControlledFragmentHandler"/> that reassembles and forwards whole messages.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            byte flags = header.Flags;

            var action = ControlledFragmentHandlerAction.CONTINUE;

            if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
            {
                action = _delegate.OnFragment(buffer, offset, length, header);
            }
            else
            {
                if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) == FrameDescriptor.BEGIN_FRAG_FLAG)
                {
                    _builder.Reset().Append(buffer, offset, length);
                }
                else
                {
                    int limit = _builder.Limit();
                    _builder.Append(buffer, offset, length);

                    if ((flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
                    {
                        int msgLength = _builder.Limit();
                        action = _delegate.OnFragment(_builder.Buffer(), 0, msgLength, header);

                        if (ControlledFragmentHandlerAction.ABORT == action)
                        {
                            _builder.Limit(limit);
                        }
                        else
                        {
                            _builder.Reset();
                        }
                    }
                }
            }

            return action;
        }
    }
}
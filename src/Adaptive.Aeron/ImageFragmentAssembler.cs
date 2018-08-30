/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron
{
    /// <summary>
    /// A <seealso cref="IFragmentHandler"/> that sits in a chain-of-responsibility pattern that reassembles fragmented messages
    /// so that the next handler in the chain only sees whole messages.
    /// 
    /// Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
    /// buffer for reassembly before delegation.
    /// 
    /// The <seealso cref="Header"/> passed to the _delegate on assembling a message will be that of the last fragment.
    /// 
    /// Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
    /// When sessions go inactive see <seealso cref="UnavailableImageHandler"/>, it is possible to free the buffer by calling
    /// <seealso cref="FreeSessionBuffer(int)"/>.
    /// 
    /// <see cref="Subscription.Poll"/>
    /// <see cref="Image.Poll"/>
    /// </summary>
    public class ImageFragmentAssembler : IFragmentHandler
    {
        private readonly IFragmentHandler _delegate;
        private readonly BufferBuilder _builder;

        /// <summary>
        /// Construct an adapter to reassemble message fragments and _delegate on only whole messages.
        /// </summary>
        /// <param name="fragmentHandler">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public ImageFragmentAssembler(IFragmentHandler fragmentHandler, int initialBufferLength = 0)
        {
            _delegate = fragmentHandler;
            _builder = new BufferBuilder(initialBufferLength);
        }

        /// <summary>
        /// Construct an adapter to reassemble message fragments and _delegate on only whole messages.
        /// </summary>
        /// <param name="fragmentHandler">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public ImageFragmentAssembler(FragmentHandler fragmentHandler, int initialBufferLength = 0)
        {
            _delegate = HandlerHelper.ToFragmentHandler(fragmentHandler);
            _builder = new BufferBuilder(initialBufferLength);
        }

        /// <summary>
        /// Get the delegate unto which assembled messages are delegated.
        /// </summary>
        /// <returns> the delegate unto which assembled messages are delegated.</returns>
        public IFragmentHandler Delegate()
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
        /// The implementation of <seealso cref="FragmentHandler"/> that reassembles and forwards whole messages.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            byte flags = header.Flags;

            if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
            {
                _delegate.OnFragment(buffer, offset, length, header);
            }
            else
            {
                HandleFragment(buffer, offset, length, header, flags);
            }
        }

        private void HandleFragment(IDirectBuffer buffer, int offset, int length, Header header, byte flags)
        {
            if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) == FrameDescriptor.BEGIN_FRAG_FLAG)
            {
                _builder.Reset().Append(buffer, offset, length);
            }
            else
            {

                _builder.Append(buffer, offset, length);

                if ((flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
                {
                    int msgLength = _builder.Limit();
                    _delegate.OnFragment(_builder.Buffer(), 0, msgLength, header);
                    _builder.Reset();
                }
            }
        }
    }
}
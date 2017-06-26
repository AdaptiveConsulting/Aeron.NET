﻿/*
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

using System;
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// A <seealso cref="IControlledFragmentHandler"/> that sits in a chain-of-responsibility pattern that reassembles fragmented
    /// messages so that the next handler in the chain only sees whole messages.
    /// 
    /// Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
    /// buffer for reassembly before delegation.
    /// 
    /// The <seealso cref="Header"/> passed to the delegate on assembling a message will be that of the last fragment.
    /// 
    /// Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
    /// When sessions go inactive see <seealso cref="UnavailableImageHandler"/>, it is possible to free the buffer by calling
    /// <seealso cref="FreeSessionBuffer(int)"/>.
    /// </summary>
    /// <seealso cref="Subscription.ControlledPoll"/>
    /// <seealso cref="Image.ControlledPoll"/>
    /// <seealso cref="Image.ControlledPeek"/>
    public class ControlledFragmentAssembler : IControlledFragmentHandler
    {
        private readonly int _initialBufferLength;
        private readonly IControlledFragmentHandler _delegate;
        private readonly IDictionary<int, BufferBuilder> _builderBySessionIdMap = new Dictionary<int, BufferBuilder>();

        /// <summary>
        /// Construct an adapter to reassembly message fragments and delegate on only whole messages.
        /// </summary>
        /// <param name="delegate">            onto which whole messages are forwarded. </param>
        /// <param name="initialBufferLength"> to be used for each session. </param>
        public ControlledFragmentAssembler(IControlledFragmentHandler @delegate, int initialBufferLength = BufferBuilder.INITIAL_CAPACITY)
        {
            _initialBufferLength = initialBufferLength;
            _delegate = @delegate;
        }

        /// <summary>
        /// Get the delegate unto which assembled messages are delegated.
        /// </summary>
        /// <returns>  the delegate unto which assembled messages are delegated. </returns>
        public virtual IControlledFragmentHandler Delegate()
        {
            return _delegate;
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
                    BufferBuilder builder;
                    if (!_builderBySessionIdMap.TryGetValue(header.SessionId, out builder))
                    {
                        builder = GetBufferBuilder(header.SessionId);
                        _builderBySessionIdMap[header.SessionId] = builder;
                    }
                    builder.Reset().Append(buffer, offset, length);
                }
                else
                {
                    BufferBuilder builder;
                    if (_builderBySessionIdMap.TryGetValue(header.SessionId, out builder))
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

        /// <summary>
        /// Clear down the cache of buffers by session for reassembling messages.
        /// </summary>
        public void Clear()
        {
            _builderBySessionIdMap.Clear();
        }

        private BufferBuilder GetBufferBuilder(int sessionId)
        {
            BufferBuilder bufferBuilder;

            _builderBySessionIdMap.TryGetValue(sessionId, out bufferBuilder);

            if (null == bufferBuilder)
            {
                bufferBuilder = new BufferBuilder(_initialBufferLength);
                _builderBySessionIdMap[sessionId] = bufferBuilder;
            }
            
            return bufferBuilder;
        }
    }
}
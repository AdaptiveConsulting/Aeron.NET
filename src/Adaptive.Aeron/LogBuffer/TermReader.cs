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

using System;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// A term buffer reader.
    /// <para>
    /// <b>Note:</b> Reading from the term is thread safe, but each thread needs its own instance of this class.
    /// </para>
    /// </summary>
    public class TermReader
    {
        /// <summary>
        /// Reads data from a term in a log buffer and updates a passed <seealso cref="IPosition"/> so progress is not lost in the
        /// event of an exception.
        ///     
        /// If a fragmentsLimit of 0 or less is passed then at least one read will be attempted.
        /// </summary>
        /// <param name="termBuffer">         to be read for fragments. </param>
        /// <param name="termOffset">         within the buffer that the read should begin. </param>
        /// <param name="handler">            the handler for data that has been read </param>
        /// <param name="fragmentsLimit">     limit the number of fragments read. </param>
        /// <param name="header">             to be used for mapping over the header for a given fragment. </param>
        /// <param name="errorHandler">       to be notified if an error occurs during the callback. </param>
        /// <param name="currentPosition">    prior to reading further fragments </param>
        /// <param name="subscriberPosition"> to be updated after reading with new position </param>
        /// <returns> the number of fragments read </returns>
        public static int Read(UnsafeBuffer termBuffer, int termOffset, FragmentHandler handler, int fragmentsLimit, Header header, ErrorHandler errorHandler, long currentPosition, IPosition subscriberPosition)
        {
            int fragmentsRead = 0;
            int offset = termOffset;
            int capacity = termBuffer.Capacity;
            header.Buffer = termBuffer;

            try
            {
                do
                {
                    int frameLength = FrameDescriptor.FrameLengthVolatile(termBuffer, offset);
                    if (frameLength <= 0)
                    {
                        break;
                    }

                    //JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
                    //ORIGINAL LINE: final int frameOffset = offset;
                    int frameOffset = offset;
                    offset += BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    if (!FrameDescriptor.IsPaddingFrame(termBuffer, frameOffset))
                    {
                        header.Offset= frameOffset;

                        handler(termBuffer, frameOffset + DataHeaderFlyweight.HEADER_LENGTH, frameLength - DataHeaderFlyweight.HEADER_LENGTH, header);

                        ++fragmentsRead;
                    }
                } while (fragmentsRead < fragmentsLimit && offset < capacity);
            }

            catch (Exception t)
            {
                errorHandler(t);
            }
            finally
            {
                long newPosition = currentPosition + (offset - termOffset);
                if (newPosition > currentPosition)
                {
                    subscriberPosition.SetOrdered(newPosition);
                }
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Reads data from a term in a log buffer.
        /// 
        /// If a fragmentsLimit of 0 or less is passed then at least one read will be attempted.
        /// </summary>
        /// <param name="termBuffer">     to be read for fragments. </param>
        /// <param name="offset">         offset within the buffer that the read should begin. </param>
        /// <param name="handler">        the handler for data that has been read </param>
        /// <param name="fragmentsLimit"> limit the number of fragments read. </param>
        /// <param name="header">         to be used for mapping over the header for a given fragment. </param>
        /// <param name="errorHandler">   to be notified if an error occurs during the callback. </param>
        /// <returns> the number of fragments read </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long Read(UnsafeBuffer termBuffer, int offset, FragmentHandler handler, int fragmentsLimit, Header header, ErrorHandler errorHandler)
        {
            int fragmentsRead = 0;
            int capacity = termBuffer.Capacity;

            try
            {
                do
                {
                    int frameLength = FrameDescriptor.FrameLengthVolatile(termBuffer, offset);
                    if (frameLength <= 0)
                    {
                        break;
                    }

                    int termOffset = offset;
                    offset += BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    if (!FrameDescriptor.IsPaddingFrame(termBuffer, termOffset))
                    {
                        header.SetBuffer(termBuffer, termOffset);

                        handler(termBuffer, termOffset + DataHeaderFlyweight.HEADER_LENGTH, frameLength - DataHeaderFlyweight.HEADER_LENGTH, header);

                        ++fragmentsRead;
                    }
                } while (fragmentsRead < fragmentsLimit && offset < capacity);
            }
            catch (Exception t)
            {
                errorHandler(t);
            }

            return Pack(offset, fragmentsRead);
        }

        /// <summary>
        /// Pack the values for fragmentsRead and offset into a long for returning on the stack.
        /// </summary>
        /// <param name="offset">        value to be packed. </param>
        /// <param name="fragmentsRead"> value to be packed. </param>
        /// <returns> a long with both ints packed into it. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long Pack(int offset, int fragmentsRead)
        {
            return ((long)offset << 32) | (uint)fragmentsRead;
        }

        /// <summary>
        /// The number of fragments that have been read.
        /// </summary>
        /// <param name="readOutcome"> into which the fragments read value has been packed. </param>
        /// <returns> the number of fragments that have been read. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FragmentsRead(long readOutcome)
        {
            return (int)readOutcome;
        }

        /// <summary>
        /// The offset up to which the term has progressed.
        /// </summary>
        /// <param name="readOutcome"> into which the offset value has been packed. </param>
        /// <returns> the offset up to which the term has progressed. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Offset(long readOutcome)
        {
            return (int)((long)((ulong)readOutcome >> 32));
        }
    }
}
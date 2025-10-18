﻿/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Abstraction over a range of buffer types that allows type to be accessed with memory ordering semantics.
    /// </summary>
    public interface IAtomicBuffer : IMutableDirectBuffer
    {
        /// <summary>
        /// Verify that the underlying buffer is correctly aligned to prevent word tearing and other ordering issues.
        /// </summary>
        /// <exception cref="InvalidOperationException"> if the alignment is not correct. </exception>
        void VerifyAlignment();

        /// <summary>
        /// Get the value at a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        long GetLongVolatile(int index);

        /// <summary>
        /// Put a value to a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutLongVolatile(int index, long value);

        /// <summary>
        /// Put a value to a given index with ordered store semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutLongOrdered(int index, long value);
        
        /// <summary>
        /// Put a value to a given index with release semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutLongRelease(int index, long value);

        /// <summary>
        /// Add a value to a given index with ordered store semantics. Use a negative increment to decrement. </summary>
        /// <param name="index">     in bytes for where to put. </param>
        /// <param name="increment"> by which the value at the index will be adjusted. </param>
        /// <returns> the previous value at the index </returns>
        long AddLongOrdered(int index, long increment);

        /// <summary>
        /// Atomic compare and set of a long given an expected value.
        /// </summary>
        /// <param name="index">         in bytes for where to put. </param>
        /// <param name="expectedValue"> at to be compared </param>
        /// <param name="updateValue">   to be exchanged </param>
        /// <returns> set successful or not </returns>
        bool CompareAndSetLong(int index, long expectedValue, long updateValue);

        /// <summary>
        /// Atomically add a delta to a value at a location returning the previous contents.
        /// To decrement a negative delta can be provided.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="delta"> to be added to the value at the index </param>
        /// <returns> previous value </returns>
        long GetAndAddLong(int index, long delta);

        /// <summary>
        /// Get the value at a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        int GetIntVolatile(int index);

        /// <summary>
        /// Put a value to a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutIntVolatile(int index, int value);

        /// <summary>
        /// Put a value to a given index with ordered semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutIntOrdered(int index, int value);
        
        /// <summary>
        /// Put a value to a given index with release semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutIntRelease(int index, int value);

        /// <summary>
        /// Add a value to a given index with ordered store semantics. Use a negative increment to decrement. </summary>
        /// <param name="index">     in bytes for where to put. </param>
        /// <param name="increment"> by which the value at the index will be adjusted. </param>
        /// <returns> the previous value at the index </returns>
        int AddIntOrdered(int index, int increment);

        /// <summary>
        /// Atomic compare and set of a int given an expected value.
        /// </summary>
        /// <param name="index">         in bytes for where to put. </param>
        /// <param name="expectedValue"> at to be compared </param>
        /// <param name="updateValue">   to be exchanged </param>
        /// <returns> successful or not </returns>
        bool CompareAndSetInt(int index, int expectedValue, int updateValue);

        /// <summary>
        /// Atomically add a delta to a value at a location returning the previous contents.
        /// To decrement a negative delta can be provided.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="delta"> to be added to the value at the index </param>
        /// <returns> previous value </returns>
        int GetAndAddInt(int index, int delta);

        /// <summary>
        /// Get the value at a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        short GetShortVolatile(int index);

        /// <summary>
        /// Put a value to a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutShortVolatile(int index, short value);

        /// <summary>
        /// Get the value at a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        byte GetByteVolatile(int index);

        /// <summary>
        /// Put a value to a given index with volatile semantics.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutByteVolatile(int index, byte value);

    }
}
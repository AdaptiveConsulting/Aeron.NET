﻿/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

namespace Adaptive.Agrona.Concurrent.Status
{

	/// <summary>
	/// Reports a position by recording it in an <seealso cref="UnsafeBuffer"/>.
	/// </summary>
	public sealed class UnsafeBufferPosition : IPosition
	{
		private readonly int _counterId;
		private readonly int _offset;
		private readonly UnsafeBuffer _buffer;
		private readonly CountersManager _countersManager;

		/// <summary>
		/// Map a position over a buffer.
		/// </summary>
		/// <param name="buffer">    containing the counter. </param>
		/// <param name="counterId"> identifier of the counter. </param>
		public UnsafeBufferPosition(UnsafeBuffer buffer, int counterId) : this(buffer, counterId, null)
		{
		}

		/// <summary>
		/// Map a position over a buffer and this indicator owns the counter for reclamation.
		/// </summary>
		/// <param name="buffer">          containing the counter. </param>
		/// <param name="counterId">       identifier of the counter. </param>
		/// <param name="countersManager"> to be used for freeing the counter when this is closed. </param>
		public UnsafeBufferPosition(UnsafeBuffer buffer, int counterId, CountersManager countersManager)
		{
			_buffer = buffer;
			_counterId = counterId;
			_countersManager = countersManager;
			_offset = CountersReader.CounterOffset(counterId);
		}

		public int Id()
		{
			return _counterId;
		}

		public long Get()
		{
			return _buffer.GetLong(_offset);
		}

		public long Volatile => _buffer.GetLongVolatile(_offset);

	    public void Set(long value)
		{
			_buffer.PutLong(_offset, value);
		}

	    public void SetOrdered(long value)
	    {
	        _buffer.PutLongOrdered(_offset, value);
	    }

	    public bool ProposeMax(long proposedValue)
		{
			UnsafeBuffer buffer = _buffer;
			int offset = _offset;
			bool updated = false;

			if (buffer.GetLong(offset) < proposedValue)
			{
				buffer.PutLong(offset, proposedValue);
				updated = true;
			}

			return updated;
		}

		public bool ProposeMaxOrdered(long proposedValue)
		{
			UnsafeBuffer buffer = _buffer;
			int offset = _offset;
			bool updated = false;

			if (buffer.GetLong(offset) < proposedValue)
			{
				buffer.PutLongOrdered(offset, proposedValue);
				updated = true;
			}

			return updated;
		}

		public void Dispose()
		{
		    _countersManager?.Free(_counterId);
		}
	}

}
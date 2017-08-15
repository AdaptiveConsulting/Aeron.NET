/*
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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Supports regular, byte ordered, and atomic (memory ordered) access to an underlying buffer.
    /// The buffer can be a byte[] or an unmanaged buffer.
    /// 
    /// <seealso cref="ByteOrder"/> of a wrapped buffer is not applied to the <seealso cref="UnsafeBuffer"/>; <seealso cref="UnsafeBuffer"/>s are
    /// stateless and can be used concurrently. To control <seealso cref="ByteOrder"/> use the appropriate accessor method
    /// with the <seealso cref="ByteOrder"/> overload.
    /// 
    /// Note: This class has a natural ordering that is inconsistent with equals.
    /// Types my be different but equal on buffer contents.
    /// 
    /// Note: The wrap methods on this class are not thread safe. Concurrent access should only happen after a successful wrap.
    /// </summary>
    public unsafe class UnsafeBuffer : IAtomicBuffer, IDisposable
    {
        /// <summary>
        /// Buffer alignment to ensure atomic word accesses.
        /// </summary>
        public const int ALIGNMENT = BitUtil.SIZE_OF_LONG;

        private byte* _pBuffer;
        private bool _disposed;
        private GCHandle _pinnedGcHandle;
        private bool _needToFreeGcHandle;

        /// <summary>
        /// Attach a view to a byte[] for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        public UnsafeBuffer(byte[] buffer)
        {
            Wrap(buffer);
        }

        public UnsafeBuffer()
        {
            Capacity = -1; // this is only used in the test, by the mock infrastructure
        }

        /// <summary>
        /// Attach a view to a byte[] for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        /// <param name="offset"> within the buffer to begin. </param>
        /// <param name="length"> of the buffer to be included. </param>
        public UnsafeBuffer(byte[] buffer, int offset, int length)
        {
            Wrap(buffer, offset, length);
        }

        /// <summary>
        /// Attach a view to a bytebuffer for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        public UnsafeBuffer(ByteBuffer buffer)
        {
            Wrap(buffer);
        }


        /// <summary>
        /// Attach a view to an existing <seealso cref="IDirectBuffer"/>
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        public UnsafeBuffer(IDirectBuffer buffer)
        {
            Wrap(buffer);
        }

        /// <summary>
        /// Attach a view to an existing <seealso cref="IDirectBuffer"/>
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        /// <param name="offset"> within the buffer to begin. </param>
        /// <param name="length"> of the buffer to be included. </param>
        public UnsafeBuffer(IDirectBuffer buffer, int offset, int length)
        {
            Wrap(buffer, offset, length);
        }

        /// <summary>
        /// Attach a view to an off-heap memory region by address.
        /// </summary>
        /// <param name="address"> where the memory begins off-heap </param>
        /// <param name="length">  of the buffer from the given address </param>
        public UnsafeBuffer(IntPtr address, int length)
        {
            Wrap(address, length);
        }

        /// <summary>
        /// Attach a view to an off-heap memory region by address.
        /// </summary>
        /// <param name="address"> where the memory begins off-heap </param>
        /// <param name="offset"></param>
        /// <param name="length">  of the buffer from the given address </param>
        public UnsafeBuffer(IntPtr address, int offset, int length)
        {
            Wrap(address, offset, length);
        }

#if DEBUG
        public virtual void Wrap(byte[] buffer)
#else
        public void Wrap(byte[] buffer)
#endif
        {
            if (buffer == null)
            {
                ThrowHelper.ThrowArgumentNullException(nameof(buffer));
            }

            FreeGcHandle();

            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _needToFreeGcHandle = true;

            _pBuffer = (byte*) _pinnedGcHandle.AddrOfPinnedObject().ToPointer();
            Capacity = buffer.Length;

            ByteArray = buffer;
            ByteBuffer = null;
        }

#if DEBUG
        public virtual void Wrap(byte[] buffer, int offset, int length)
#else
        public void Wrap(byte[] buffer, int offset, int length)
#endif
        {
            if (buffer == null)
            {
                ThrowHelper.ThrowArgumentException(nameof(buffer));
            }

#if SHOULD_BOUNDS_CHECK
            int bufferLength = buffer.Length;
            if (offset != 0 && (offset < 0 || offset > bufferLength - 1))
            {
                ThrowHelper.ThrowArgumentException("offset=" + offset + " not valid for capacity=" + bufferLength);
            }

            if (length < 0 || length > bufferLength - offset)
            {
                ThrowHelper.ThrowArgumentException("offset=" + offset + " length=" + length + " not valid for capacity=" + bufferLength);
            }
#endif

            FreeGcHandle();

            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _needToFreeGcHandle = true;

            _pBuffer = (byte*) _pinnedGcHandle.AddrOfPinnedObject().ToPointer() + offset;
            Capacity = length;

            ByteArray = buffer;
            ByteBuffer = null;
        }

#if DEBUG
        public virtual void Wrap(IDirectBuffer buffer)
#else
        public void Wrap(IDirectBuffer buffer)
#endif
        {
            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer();
            Capacity = buffer.Capacity;

            ByteArray = buffer.ByteArray;
            ByteBuffer = buffer.ByteBuffer;
        }

#if DEBUG
        public virtual void Wrap(ByteBuffer buffer)
#else
        public void Wrap(ByteBuffer buffer)
#endif
        {
            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer();
            Capacity = buffer.Capacity;

            ByteArray = null;
            ByteBuffer = buffer;
        }

#if DEBUG
        public virtual void Wrap(IDirectBuffer buffer, int offset, int length)
#else
        public void Wrap(IDirectBuffer buffer, int offset, int length)
#endif
        {
#if SHOULD_BOUNDS_CHECK
            int bufferCapacity = buffer.Capacity;
            if (offset != 0 && (offset < 0 || offset > bufferCapacity - 1))
            {
                ThrowHelper.ThrowArgumentException("offset=" + offset + " not valid for capacity=" + bufferCapacity);
            }

            if (length < 0 || length > bufferCapacity - offset)
            {
                ThrowHelper.ThrowArgumentException("offset=" + offset + " length=" + length + " not valid for capacity=" + bufferCapacity);
            }
#endif

            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer() + offset;
            Capacity = length;

            ByteArray = buffer.ByteArray;
            ByteBuffer = buffer.ByteBuffer;
        }

#if DEBUG
        public virtual void Wrap(IntPtr pointer, int length)
#else
        public void Wrap(IntPtr pointer, int length)
#endif
        {
            FreeGcHandle();

            _needToFreeGcHandle = false;

            _pBuffer = (byte*) pointer.ToPointer();
            Capacity = length;

            ByteBuffer = null;
            ByteArray = null;
        }

#if DEBUG
        public virtual void Wrap(IntPtr pointer, int offset, int length)
#else
        public void Wrap(IntPtr pointer, int offset, int length)
#endif
        {
            FreeGcHandle();

            _needToFreeGcHandle = false;

            _pBuffer = (byte*) pointer.ToPointer() + offset;
            Capacity = length;

            ByteBuffer = null;
            ByteArray = null;
        }

#if DEBUG
        public virtual IntPtr BufferPointer => new IntPtr(_pBuffer);
#else
        public IntPtr BufferPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new IntPtr(_pBuffer); }
        }

        public byte[] ByteArray { get; private set; }

        public ByteBuffer ByteBuffer { get; private set; }

#endif

#if DEBUG
        public virtual int Capacity { get; private set; }
#else
        public int Capacity { get; private set; }
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void SetMemory(int index, int length, byte value)
#else
        public void SetMemory(int index, int length, byte value)
#endif
        {
            BoundsCheck0(index, length);
            // TODO PERF Naive implementation, we should not write byte by byte, this is slow
            //UNSAFE.SetMemory(byteArray, addressOffset + index, length, value);
            for (var i = index; i < index + length; i++)
            {
                _pBuffer[i] = value;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void CheckLimit(int limit)
#else
        public void CheckLimit(int limit)
#endif
        {
            if (limit > Capacity)
            {
                ThrowHelper.ThrowIndexOutOfRangeException($"limit={limit:D} is beyond capacity={Capacity:D}");
            }
        }

        public bool IsExpandable => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void VerifyAlignment()
#else
        public void VerifyAlignment()
#endif
        {
            var address = new IntPtr(_pBuffer).ToInt64();
            if (0 != (address & (ALIGNMENT - 1)))
            {
                ThrowHelper.ThrowInvalidOperationException(
                    $"AtomicBuffer is not correctly aligned: addressOffset={address:D} in not divisible by {ALIGNMENT:D}");
            }
        }

        ///////////////////////////////////////////////////////////////////////////

        //public long GetLong(int index, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

        //    var value = *(long*)(_pBuffer + index);
        //    return EndianessConverter.ApplyInt64(byteOrder, value);
        //}

#if DEBUG
        //public virtual void PutLong(int index, long value, ByteOrder byteOrder)
#else
        //public void PutLong(int index, long value, ByteOrder byteOrder)
#endif
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

        //    value = EndianessConverter.ApplyInt64(byteOrder, value);
        //    *(long*)(_pBuffer + index) = value;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            return *(long*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutLong(int index, long value)
#else
        public void PutLong(int index, long value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);
            *(long*) (_pBuffer + index) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual long GetLongVolatile(int index)
#else
        public long GetLongVolatile(int index)
#endif
        {
#if SHOULD_BOUNDS_CHECK
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);
#endif
            return Volatile.Read(ref *(long*) (_pBuffer + index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutLongVolatile(int index, long value)
#else
        public void PutLongVolatile(int index, long value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            Interlocked.Exchange(ref *(long*) (_pBuffer + index), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutLongOrdered(int index, long value)
#else
        public void PutLongOrdered(int index, long value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            Volatile.Write(ref *(long*) (_pBuffer + index), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual long AddLongOrdered(int index, long increment)
#else
        public long AddLongOrdered(int index, long increment)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            var value = GetLong(index);
            PutLongOrdered(index, value + increment);

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual bool CompareAndSetLong(int index, long expectedValue, long updateValue)
#else
        public bool CompareAndSetLong(int index, long expectedValue, long updateValue)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            var original = Interlocked.CompareExchange(ref *(long*) (_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual long GetAndAddLong(int index, long delta)
#else
        public long GetAndAddLong(int index, long delta)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            return Interlocked.Add(ref *(long*) (_pBuffer + index), delta) - delta;
        }

        ///////////////////////////////////////////////////////////////////////////

        //public int GetInt(int index, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_INT);

        //    var value = *(int*)(_pBuffer + index);
        //    return EndianessConverter.ApplyInt32(byteOrder, value);
        //}

#if DEBUG
        //public virtual void PutInt(int index, int value, ByteOrder byteOrder)
#else
        //public void PutInt(int index, int value, ByteOrder byteOrder)
#endif
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_INT);

        //    value = EndianessConverter.ApplyInt32(byteOrder, value);
        //    *(int*)(_pBuffer + index) = value;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int GetInt(int index)
#else
        public int GetInt(int index)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            return *(int*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutInt(int index, int value)
#else
        public void PutInt(int index, int value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);
            *(int*) (_pBuffer + index) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int GetIntVolatile(int index)
#else
        public int GetIntVolatile(int index)
#endif
        {
#if SHOULD_BOUNDS_CHECK
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);
#endif
            return Volatile.Read(ref *(int*) (_pBuffer + index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutIntVolatile(int index, int value)
#else
        public void PutIntVolatile(int index, int value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            Interlocked.Exchange(ref *(int*) (_pBuffer + index), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutIntOrdered(int index, int value)
#else
        public void PutIntOrdered(int index, int value)
#endif
        {
#if SHOULD_BOUNDS_CHECK
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);
#endif
            Volatile.Write(ref *(int*) (_pBuffer + index), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int AddIntOrdered(int index, int increment)
#else
        public int AddIntOrdered(int index, int increment)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            var value = GetInt(index);
            PutIntOrdered(index, value + increment);

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual bool CompareAndSetInt(int index, int expectedValue, int updateValue)
#else
        public bool CompareAndSetInt(int index, int expectedValue, int updateValue)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            var original = Interlocked.CompareExchange(ref *(int*) (_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int GetAndAddInt(int index, int delta)
#else
        public int GetAndAddInt(int index, int delta)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            return Interlocked.Add(ref *(int*) (_pBuffer + index), delta) - delta;
        }

        ///////////////////////////////////////////////////////////////////////////

        // TODO Olivier: Martin told me this is not required for the client

        //public double GetDouble(int index, ByteOrder byteOrder)
        //{
        //    if (SHOULD_BOUNDS_CHECK)
        //    {
        //        BoundsCheck0(index, SIZE_OF_DOUBLE);
        //    }

        //    if (NATIVE_BYTE_ORDER != byteOrder)
        //    {
        //        long bits = UNSAFE.GetLong(byteArray, addressOffset + index);
        //        return Double.LongBitsToDouble(Long.ReverseBytes(bits));
        //    }
        //    else
        //    {
        //        return UNSAFE.GetDouble(byteArray, addressOffset + index);
        //    }
        //}

#if DEBUG
        //public virtual void PutDouble(int index, double value, ByteOrder byteOrder)
#else
        //public void PutDouble(int index, double value, ByteOrder byteOrder)
#endif
        //{
        //    if (SHOULD_BOUNDS_CHECK)
        //    {
        //        BoundsCheck0(index, SIZE_OF_DOUBLE);
        //    }

        //    if (NATIVE_BYTE_ORDER != byteOrder)
        //    {
        //        long bits = Long.ReverseBytes(Double.DoubleToRawLongBits(value));
        //        UNSAFE.PutLong(byteArray, addressOffset + index, bits);
        //    }
        //    else
        //    {
        //        UNSAFE.PutDouble(byteArray, addressOffset + index, value);
        //    }
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual double GetDouble(int index)
#else
        public double GetDouble(int index)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_DOUBLE);

            return *(double*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutDouble(int index, double value)
#else
        public void PutDouble(int index, double value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_DOUBLE);

            *(double*) (_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual float GetFloat(int index)
#else
        public float GetFloat(int index)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_FLOAT);

            return *(float*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutFloat(int index, float value)
#else
        public void PutFloat(int index, float value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_FLOAT);

            *(float*) (_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual short GetShort(int index)
#else
        public short GetShort(int index)
#endif
        {
#if SHOULD_BOUNDS_CHECK
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);
#endif
            return *(short*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutShort(int index, short value)
#else
        public void PutShort(int index, short value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            *(short*) (_pBuffer + index) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual short GetShortVolatile(int index)
#else
        public short GetShortVolatile(int index)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            return Volatile.Read(ref *(short*) (_pBuffer + index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutShortVolatile(int index, short value)
#else
        public void PutShortVolatile(int index, short value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            Volatile.Write(ref (*(short*) (_pBuffer + index)), value);
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual byte GetByte(int index)
#else
        public byte GetByte(int index)
#endif
        {
            BoundsCheck(index);

            return *(_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutByte(int index, byte value)
#else
        public void PutByte(int index, byte value)
#endif
        {
            BoundsCheck(index);

            *(_pBuffer + index) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual byte GetByteVolatile(int index)
#else
        public byte GetByteVolatile(int index)
#endif
        {
            BoundsCheck(index);

            return Volatile.Read(ref *(_pBuffer + index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutByteVolatile(int index, byte value)
#else
        public void PutByteVolatile(int index, byte value)
#endif
        {
            BoundsCheck(index);

            Volatile.Write(ref *(_pBuffer + index), value);
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void GetBytes(int index, byte[] dst)
#else
        public void GetBytes(int index, byte[] dst)
#endif
        {
            GetBytes(index, dst, 0, dst.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void GetBytes(int index, byte[] dst, int offset, int length)
#else
        public void GetBytes(int index, byte[] dst, int offset, int length)
#endif
        {
            BoundsCheck0(index, length);
            BufferUtil.BoundsCheck(dst, offset, length);

            byte* source = _pBuffer + index;
            fixed (byte* destination = &dst[offset])
            {
                ByteUtil.MemoryCopy(destination, source, (uint) length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length)
#else
        public void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length)
#endif
        {
            dstBuffer.PutBytes(dstIndex, this, index, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutBytes(int index, byte[] src)
#else
        public void PutBytes(int index, byte[] src)
#endif
        {
            PutBytes(index, src, 0, src.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutBytes(int index, byte[] src, int offset, int length)
#else
        public void PutBytes(int index, byte[] src, int offset, int length)
#endif
        {
            BoundsCheck0(index, length);
            BufferUtil.BoundsCheck(src, offset, length);

            byte* destination = _pBuffer + index;
            fixed (byte* source = &src[offset])
            {
                ByteUtil.MemoryCopy(destination, source, (uint) length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
#else
        public void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
#endif
        {
            BoundsCheck0(index, length);
            srcBuffer.BoundsCheck(srcIndex, length);

            byte* destination = _pBuffer + index;
            byte* source = (byte*) srcBuffer.BufferPointer.ToPointer() + srcIndex;
            ByteUtil.MemoryCopy(destination, source, (uint) length);
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual char GetChar(int index)
#else
        public char GetChar(int index)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

            return *(char*) (_pBuffer + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void PutChar(int index, char value)
#else
        public void PutChar(int index, char value)
#endif
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

            *(char*) (_pBuffer + index) = value;
        }

        //public char GetCharVolatile(int index)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

        //    return (char)Volatile.Read(ref *(short*)(_pBuffer + index));
        //}

#if DEBUG
        //public virtual void PutCharVolatile(int index, char value)
#else
        //public void PutCharVolatile(int index, char value)
#endif
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

        //    Interlocked.Exchange(ref *(short*)(_pBuffer + index), (short)value);
        //}

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual string GetStringUtf8(int index)
#else
        public string GetStringUtf8(int index)
#endif
        {
            var length = GetInt(index);

            return GetStringUtf8(index, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual string GetStringAscii(int index)
#else
        public string GetStringAscii(int index)
#endif
        {
            var length = GetInt(index);

            return GetStringAscii(index, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual string GetStringUtf8(int index, int length)
#else
        public string GetStringUtf8(int index, int length)
#endif
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SIZE_OF_INT, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual string GetStringAscii(int index, int length)
#else
        public string GetStringAscii(int index, int length)
#endif
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SIZE_OF_INT, stringInBytes);

            return Encoding.ASCII.GetString(stringInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PutStringUtf8(int index, string value)
        {
            return PutStringUtf8(index, value, int.MaxValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PutStringAscii(int index, string value)
        {
            return PutStringAscii(index, value, int.MaxValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PutStringUtf8(int index, string value, int maxEncodedSize)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);
            if (bytes.Length > maxEncodedSize)
            {
                ThrowHelper.ThrowArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
            }

            PutInt(index, bytes.Length);
            PutBytes(index + BitUtil.SIZE_OF_INT, bytes);

            return BitUtil.SIZE_OF_INT + bytes.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PutStringAscii(int index, string value, int maxEncodedSize)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.ASCII.GetBytes(value);
            if (bytes.Length > maxEncodedSize)
            {
                ThrowHelper.ThrowArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
            }

            PutInt(index, bytes.Length);
            PutBytes(index + BitUtil.SIZE_OF_INT, bytes);

            return BitUtil.SIZE_OF_INT + bytes.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual string GetStringWithoutLengthUtf8(int index, int length)
#else
        public string GetStringWithoutLengthUtf8(int index, int length)
#endif
        {
            var stringInBytes = new byte[length];
            GetBytes(index, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int PutStringWithoutLengthUtf8(int index, string value)
#else
        public int PutStringWithoutLengthUtf8(int index, string value)
#endif
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);
            PutBytes(index, bytes);

            return bytes.Length;
        }

        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BoundsCheck(int index)
        {
#if SHOULD_BOUNDS_CHECK
            if (index < 0 || index >= Capacity)
            {
                ThrowHelper.ThrowIndexOutOfRangeException($"index={index:D}, capacity={Capacity:D}");
            }
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BoundsCheck0(int index, int length)
        {
#if SHOULD_BOUNDS_CHECK
            long resultingPosition = index + (long)length;
            if (index < 0 || resultingPosition > Capacity)
            {
                ThrowHelper.ThrowIndexOutOfRangeException($"index={index:D}, length={length:D}, capacity={Capacity:D}");
            }
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void BoundsCheck(int index, int length)
#else
        public void BoundsCheck(int index, int length)
#endif
        {
            BoundsCheck0(index, length);
        }
        
        ///////////////////////////////////////////////////////////////////////////

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int CompareTo(IDirectBuffer that)
#else
        public int CompareTo(IDirectBuffer that)
#endif
        {
            var thisCapacity = this.Capacity;
            var thatCapacity = that.Capacity;

            var thisPointer = this._pBuffer;
            var thatPointer = (byte*) that.BufferPointer.ToPointer();

            for (int i = 0, length = Math.Min(thisCapacity, thatCapacity); i < length; i++)
            {
                var cmp = thisPointer[i] - thatPointer[i];

                if (0 != cmp)
                {
                    return cmp;
                }
            }

            if (thisCapacity != thatCapacity)
            {
                return thisCapacity - thatCapacity;
            }

            return 0;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
#if DEBUG
        public virtual void Dispose()
#else
        public void Dispose()
#endif
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Destructor for <see cref="UnsafeBuffer"/>
        /// </summary>
        ~UnsafeBuffer()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            FreeGcHandle();

            ByteArray = null;
            ByteBuffer = null;
            
            _disposed = true;
        }

        private void FreeGcHandle()
        {
            if (_needToFreeGcHandle)
            {
                _pinnedGcHandle.Free();
                _needToFreeGcHandle = false;
            }
        }
    }
}
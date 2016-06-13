using System;
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

        public void Wrap(byte[] buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));

            FreeGcHandle();

            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _needToFreeGcHandle = true;

            _pBuffer = (byte*) _pinnedGcHandle.AddrOfPinnedObject().ToPointer();
            Capacity = buffer.Length;
        }

        public void Wrap(byte[] buffer, int offset, int length)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));

#if SHOULD_BOUNDS_CHECK
            int bufferLength = buffer.Length;
            if (offset != 0 && (offset < 0 || offset > bufferLength - 1))
            {
                throw new ArgumentException("offset=" + offset + " not valid for buffer.length=" + bufferLength);
            }

            if (length < 0 || length > bufferLength - offset)
            {
                throw new ArgumentException("offset=" + offset + " length=" + length + " not valid for buffer.length=" + bufferLength);
            }
#endif

            FreeGcHandle();

            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _needToFreeGcHandle = true;

            _pBuffer = (byte*) _pinnedGcHandle.AddrOfPinnedObject().ToPointer() + offset;
            Capacity = length;
        }

        public void Wrap(IDirectBuffer buffer)
        {
            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer();
            Capacity = buffer.Capacity;
        }

        public void Wrap(ByteBuffer buffer)
        {
            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer();
            Capacity = buffer.Capacity;
        }

        public void Wrap(IDirectBuffer buffer, int offset, int length)
        {
#if SHOULD_BOUNDS_CHECK
            int bufferCapacity = buffer.Capacity;
            if (offset != 0 && (offset < 0 || offset > bufferCapacity - 1))
            {
                throw new ArgumentException("offset=" + offset + " not valid for buffer.capacity()=" + bufferCapacity);
            }

            if (length < 0 || length > bufferCapacity - offset)
            {
                throw new ArgumentException("offset=" + offset + " length=" + length + " not valid for buffer.capacity()=" + bufferCapacity);
            }
#endif

            FreeGcHandle();
            _needToFreeGcHandle = false;

            _pBuffer = (byte*) buffer.BufferPointer.ToPointer() + offset;
            Capacity = length;
        }

        public void Wrap(IntPtr pointer, int length)
        {
            FreeGcHandle();

            _needToFreeGcHandle = false;

            _pBuffer = (byte*) pointer.ToPointer();
            Capacity = length;
        }

        public void Wrap(IntPtr pointer, int offset, int length)
        {
            FreeGcHandle();

            _needToFreeGcHandle = false;

            _pBuffer = (byte*) pointer.ToPointer() + offset;
            Capacity = length;
        }

        public IntPtr BufferPointer => new IntPtr(_pBuffer);

        public int Capacity { get; private set; }

        public void SetMemory(int index, int length, byte value)
        {
            BoundsCheck0(index, length);

            // TODO PERF Naive implementation, we should not write byte by byte, this is slow
            //UNSAFE.SetMemory(byteArray, addressOffset + index, length, value);
            for (var i = index; i < index + length; i++)
            {
                _pBuffer[i] = value;
            }
        }

        public void CheckLimit(int limit)
        {
            if (limit > Capacity)
            {
                throw new IndexOutOfRangeException($"limit={limit:D} is beyond capacity={Capacity:D}");
            }
        }

        public bool IsExpandable => false;

        public void VerifyAlignment()
        {
            var address = new IntPtr(_pBuffer).ToInt64();
            if (0 != (address & (ALIGNMENT - 1)))
            {
                throw new InvalidOperationException(
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

        //public void PutLong(int index, long value, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

        //    value = EndianessConverter.ApplyInt64(byteOrder, value);
        //    *(long*)(_pBuffer + index) = value;
        //}

        public long GetLong(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            return *(long*) (_pBuffer + index);
        }

        public void PutLong(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);
            *(long*) (_pBuffer + index) = value;
        }

        public long GetLongVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            return Volatile.Read(ref *(long*) (_pBuffer + index));
        }

        public void PutLongVolatile(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            Interlocked.Exchange(ref *(long*) (_pBuffer + index), value);
        }

        public void PutLongOrdered(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            Volatile.Write(ref *(long*) (_pBuffer + index), value);
        }

        public long AddLongOrdered(int index, long increment)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            var value = GetLong(index);
            PutLongOrdered(index, value + increment);

            return value;
        }

        public bool CompareAndSetLong(int index, long expectedValue, long updateValue)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            var original = Interlocked.CompareExchange(ref *(long*) (_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        public long GetAndAddLong(int index, long delta)
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

        //public void PutInt(int index, int value, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_INT);

        //    value = EndianessConverter.ApplyInt32(byteOrder, value);
        //    *(int*)(_pBuffer + index) = value;
        //}

        public int GetInt(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            return *(int*) (_pBuffer + index);
        }

        public void PutInt(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);
            *(int*) (_pBuffer + index) = value;
        }

        public int GetIntVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            return Volatile.Read(ref *(int*) (_pBuffer + index));
        }

        public void PutIntVolatile(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            Interlocked.Exchange(ref *(int*) (_pBuffer + index), value);
        }

        public void PutIntOrdered(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            Volatile.Write(ref *(int*) (_pBuffer + index), value);
        }

        public int AddIntOrdered(int index, int increment)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            var value = GetInt(index);
            PutIntOrdered(index, value + increment);

            return value;
        }

        public bool CompareAndSetInt(int index, int expectedValue, int updateValue)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            var original = Interlocked.CompareExchange(ref *(int*) (_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        public int GetAndAddInt(int index, int delta)
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

        //public void PutDouble(int index, double value, ByteOrder byteOrder)
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

        public double GetDouble(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_DOUBLE);

            return *(double*) (_pBuffer + index);
        }

        public void PutDouble(int index, double value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_DOUBLE);

            *(double*) (_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        public float GetFloat(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_FLOAT);

            return *(float*) (_pBuffer + index);
        }

        public void PutFloat(int index, float value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_FLOAT);

            *(float*) (_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        public short GetShort(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            return *(short*) (_pBuffer + index);
        }

        public void PutShort(int index, short value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            *(short*) (_pBuffer + index) = value;
        }

        public short GetShortVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            return Volatile.Read(ref *(short*) (_pBuffer + index));
        }

        public void PutShortVolatile(int index, short value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            Volatile.Write(ref (*(short*) (_pBuffer + index)), value);
        }

        ///////////////////////////////////////////////////////////////////////////

        public byte GetByte(int index)
        {
            BoundsCheck(index);

            return *(_pBuffer + index);
        }

        public void PutByte(int index, byte value)
        {
            BoundsCheck(index);

            *(_pBuffer + index) = value;
        }

        public byte GetByteVolatile(int index)
        {
            BoundsCheck(index);

            return Volatile.Read(ref *(_pBuffer + index));
        }

        public void PutByteVolatile(int index, byte value)
        {
            BoundsCheck(index);

            Volatile.Write(ref *(_pBuffer + index), value);
        }

        ///////////////////////////////////////////////////////////////////////////

        public void GetBytes(int index, byte[] dst)
        {
            GetBytes(index, dst, 0, dst.Length);
        }

        public void GetBytes(int index, byte[] dst, int offset, int length)
        {
            BoundsCheck0(index, length);
            BufferUtil.BoundsCheck(dst, offset, length);

            void* source = _pBuffer + index;
            fixed (void* destination = &dst[offset])
            {
                ByteUtil.MemoryCopy(destination, source, (uint) length);
            }
        }

        public void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length)
        {
            dstBuffer.PutBytes(dstIndex, this, index, length);
        }

        public void PutBytes(int index, byte[] src)
        {
            PutBytes(index, src, 0, src.Length);
        }

        public void PutBytes(int index, byte[] src, int offset, int length)
        {
            BoundsCheck0(index, length);
            BufferUtil.BoundsCheck(src, offset, length);

            void* destination = _pBuffer + index;
            fixed (void* source = &src[offset])
            {
                ByteUtil.MemoryCopy(destination, source, (uint) length);
            }
        }

        public void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            BoundsCheck0(index, length);
            srcBuffer.BoundsCheck(srcIndex, length);

            void* destination = _pBuffer + index;
            void* source = (byte*) srcBuffer.BufferPointer.ToPointer() + srcIndex;
            ByteUtil.MemoryCopy(destination, source, (uint) length);
        }

        ///////////////////////////////////////////////////////////////////////////

        public char GetChar(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

            return *(char*) (_pBuffer + index);
        }

        public void PutChar(int index, char value)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

            *(char*) (_pBuffer + index) = value;
        }

        //public char GetCharVolatile(int index)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

        //    return (char)Volatile.Read(ref *(short*)(_pBuffer + index));
        //}

        //public void PutCharVolatile(int index, char value)
        //{
        //    BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

        //    Interlocked.Exchange(ref *(short*)(_pBuffer + index), (short)value);
        //}

        ///////////////////////////////////////////////////////////////////////////

        public string GetStringUtf8(int index)
        {
            var length = GetInt(index);

            return GetStringUtf8(index, length);
        }

        public string GetStringUtf8(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SIZE_OF_INT, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        public int PutStringUtf8(int index, string value)
        {
            return PutStringUtf8(index, value, int.MaxValue);
        }

        public int PutStringUtf8(int index, string value, int maxEncodedSize)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);
            if (bytes.Length > maxEncodedSize)
            {
                throw new ArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
            }

            PutInt(index, bytes.Length);
            PutBytes(index + BitUtil.SIZE_OF_INT, bytes);

            return BitUtil.SIZE_OF_INT + bytes.Length;
        }

        public string GetStringWithoutLengthUtf8(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        public int PutStringWithoutLengthUtf8(int index, string value)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);
            PutBytes(index, bytes);

            return bytes.Length;
        }

        ///////////////////////////////////////////////////////////////////////////

        private void BoundsCheck(int index)
        {
#if SHOULD_BOUNDS_CHECK
            if (index < 0 || index >= _capacity)
            {
                throw new IndexOutOfRangeException($"index={index:D}, capacity={_capacity:D}");
            }
#endif
        }

        private void BoundsCheck0(int index, int length)
        {
#if SHOULD_BOUNDS_CHECK
            long resultingPosition = index + (long)length;
            if (index < 0 || resultingPosition > _capacity)
            {
                throw new IndexOutOfRangeException($"index={index:D}, length={length:D}, capacity={_capacity:D}");
            }
#endif
        }

        public void BoundsCheck(int index, int length)
        {
            BoundsCheck0(index, length);
        }

        ///////////////////////////////////////////////////////////////////////////

        public int CompareTo(IDirectBuffer that)
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
        public void Dispose()
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
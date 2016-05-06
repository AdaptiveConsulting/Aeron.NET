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
    public sealed unsafe class UnsafeBuffer : IAtomicBuffer, IDisposable
    {
        /// <summary>
        /// Buffer alignment to ensure atomic word accesses.
        /// </summary>
        public static readonly int Alignment = BitUtil.SizeOfLong;

        private byte* _pBuffer;
        private bool _disposed;
        private GCHandle _pinnedGcHandle;
        private bool _needToFreeGcHandle;
        private int _capacity;

        /// <summary>
        /// Attach a view to a byte[] for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        public UnsafeBuffer(byte[] buffer)
        {
            Wrap(buffer);
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

        public void Wrap(byte[] buffer)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));

            FreeGcHandle();

            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _needToFreeGcHandle = true;

            _pBuffer = (byte*)_pinnedGcHandle.AddrOfPinnedObject().ToPointer();
            _capacity = buffer.Length;
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

            _pBuffer = (byte*)_pinnedGcHandle.AddrOfPinnedObject().ToPointer() + offset;
            _capacity = length;
        }

        public void Wrap(IDirectBuffer buffer)
        {
            // Note Olivier: this would require to pin the underlying array (if an array was used in the source buffer)
            // will come back to this when/if we need it
            throw new NotImplementedException();
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

            // Note Olivier: this would require to pin the underlying array (if an array was used in the source buffer)
            // will come back to this when/if we need it
            throw new NotImplementedException();
        }

        public void Wrap(IntPtr pointer, int length)
        {
            FreeGcHandle();

            _needToFreeGcHandle = false;

            _pBuffer = (byte*)pointer.ToPointer();
            _capacity = length;
        }

        public IntPtr BufferPointer => new IntPtr(_pBuffer);

        public int Capacity => _capacity;

        public void SetMemory(int index, int length, byte value)
        {
            BoundsCheck0(index, length);

            // TODO PERF Naive implementation, we should not write byte by byte, this is slow
            //UNSAFE.SetMemory(byteArray, addressOffset + index, length, value);
            for (int i = index; i < index + length; i++)
            {
                _pBuffer[i] = value;
            }
        }

        public void CheckLimit(int limit)
        {
            if (limit > _capacity)
            {
                throw new IndexOutOfRangeException($"limit={limit:D} is beyond capacity={_capacity:D}");
            }
        }

        public bool IsExpandable => false;

        public void VerifyAlignment()
        {
            // TODO Olivier: port if required
            throw new NotImplementedException();
        }

        ///////////////////////////////////////////////////////////////////////////

        //public long GetLong(int index, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfLong);

        //    var value = *(long*)(_pBuffer + index);
        //    return EndianessConverter.ApplyInt64(byteOrder, value);
        //}

        //public void PutLong(int index, long value, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfLong);
            
        //    value = EndianessConverter.ApplyInt64(byteOrder, value);
        //    *(long*)(_pBuffer + index) = value;
        //}

        public long GetLong(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return *(long*)(_pBuffer + index);
        }

        public void PutLong(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);
            *(long*)(_pBuffer + index) = value;
        }

        public long GetLongVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return Volatile.Read(ref *(long*) (_pBuffer + index));
        }

        public void PutLongVolatile(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);
            
            Interlocked.Exchange(ref *(long*) (_pBuffer + index), value);
        }

        public void PutLongOrdered(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            Volatile.Write(ref *(long*) (_pBuffer + index), value);
        }

        public long AddLongOrdered(int index, long increment)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            var value = GetLong(index);
            PutLongOrdered(index, value + increment);

            return value;
        }

        public bool CompareAndSetLong(int index, long expectedValue, long updateValue)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            var original = Interlocked.CompareExchange(ref *(long*) (_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        public long GetAndSetLong(int index, long value)
        {
            // Note ODE: does not seem to be used in the codebase
            throw new NotImplementedException();
        }

        public long GetAndAddLong(int index, long delta)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return Interlocked.Add(ref *(long*)(_pBuffer + index), delta) - delta;
        }

        ///////////////////////////////////////////////////////////////////////////

        //public int GetInt(int index, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfInt);

        //    var value = *(int*)(_pBuffer + index);
        //    return EndianessConverter.ApplyInt32(byteOrder, value);
        //}

        //public void PutInt(int index, int value, ByteOrder byteOrder)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfInt);

        //    value = EndianessConverter.ApplyInt32(byteOrder, value);
        //    *(int*)(_pBuffer + index) = value;
        //}

        public int GetInt(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            return *(int*)(_pBuffer + index);
        }

        public void PutInt(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);
            *(int*)(_pBuffer + index) = value;
        }

        public int GetIntVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            return Volatile.Read(ref *(int*)(_pBuffer + index));
        }

        public void PutIntVolatile(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            Interlocked.Exchange(ref *(int*)(_pBuffer + index), value);
        }

        public void PutIntOrdered(int index, int value)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            Volatile.Write(ref *(int*)(_pBuffer + index), value);
        }

        public int AddIntOrdered(int index, int increment)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            var value = GetInt(index);
            PutIntOrdered(index, value + increment);

            return value;
        }

        public bool CompareAndSetInt(int index, int expectedValue, int updateValue)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            var original = Interlocked.CompareExchange(ref *(int*)(_pBuffer + index), updateValue, expectedValue);

            return original == expectedValue;
        }

        public int GetAndSetInt(int index, int value)
        {
            // Note ODE: does not seem to be used in the codebase
            throw new NotImplementedException();
        }


        public int GetAndAddInt(int index, int delta)
        {
            BoundsCheck0(index, BitUtil.SizeOfInt);

            return Interlocked.Add(ref *(int*)(_pBuffer + index), delta) - delta;
        }

        ///////////////////////////////////////////////////////////////////////////

        // TODO Olivier: Martin told me this is not required for the client

        //public virtual double GetDouble(int index, ByteOrder byteOrder)
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

        //public virtual void PutDouble(int index, double value, ByteOrder byteOrder)
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
            BoundsCheck0(index, BitUtil.SizeOfDouble);

            return *(double*)(_pBuffer + index);
        }

        public void PutDouble(int index, double value)
        {
            BoundsCheck0(index, BitUtil.SizeOfDouble);

            *(double*)(_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        public float GetFloat(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfFloat);

            return *(float*)(_pBuffer + index);
        }

        public void PutFloat(int index, float value)
        {
            BoundsCheck0(index, BitUtil.SizeOfFloat);

            *(float*)(_pBuffer + index) = value;
        }

        ///////////////////////////////////////////////////////////////////////////

        public short GetShort(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfShort);

            return *(short*)(_pBuffer + index);
        }

        public void PutShort(int index, short value)
        {
            BoundsCheck0(index, BitUtil.SizeOfShort);

            *(short*)(_pBuffer + index) = value;
        }

        public short GetShortVolatile(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfShort);

            return Volatile.Read(ref *(short*)(_pBuffer + index));
        }

        public void PutShortVolatile(int index, short value)
        {
            BoundsCheck0(index, BitUtil.SizeOfShort);

            Volatile.Write(ref (*(short*)(_pBuffer + index)), value);
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
                ByteUtil.MemoryCopy(destination, source, (uint)length);
            }
        }

        public void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            BoundsCheck0(index, length);
            srcBuffer.BoundsCheck(srcIndex, length);

            void* destination = _pBuffer + index;
            void* source = (byte*)srcBuffer.BufferPointer.ToPointer() + srcIndex;
            ByteUtil.MemoryCopy(destination, source, (uint)length);
        }

        ///////////////////////////////////////////////////////////////////////////
       
        public char GetChar(int index)
        {
            BoundsCheck0(index, BitUtil.SizeOfChar);

            return *(char*)(_pBuffer + index);
        }

        public void PutChar(int index, char value)
        {
            BoundsCheck0(index, BitUtil.SizeOfChar);

            *(char*)(_pBuffer + index) = value;
        }

        //public char GetCharVolatile(int index)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfChar);

        //    return (char)Volatile.Read(ref *(short*)(_pBuffer + index));
        //}

        //public void PutCharVolatile(int index, char value)
        //{
        //    BoundsCheck0(index, BitUtil.SizeOfChar);

        //    Interlocked.Exchange(ref *(short*)(_pBuffer + index), (short)value);
        //}

        ///////////////////////////////////////////////////////////////////////////

        public string GetStringUtf8(int index)
        {
            int length = GetInt(index);

            return GetStringUtf8(index, length);
        }

        public string GetStringUtf8(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SizeOfInt, stringInBytes);
            
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
            PutBytes(index + BitUtil.SizeOfInt, bytes);

            return BitUtil.SizeOfInt + bytes.Length;
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
            int thisCapacity = this.Capacity;
            int thatCapacity = that.Capacity;

            var thisPointer = this._pBuffer;
            var thatPointer = (byte*)that.BufferPointer.ToPointer();
            
            for (int i = 0, length = Math.Min(thisCapacity, thatCapacity); i < length; i++)
            {
                int cmp = thisPointer[i] - thatPointer[i];
                
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
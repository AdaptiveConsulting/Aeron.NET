using System;
using System.Runtime.InteropServices;
using System.Threading;

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

            // TODO consider moving bounds check to dedicated method(s) so we have the conditional compilation in a single place

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

            // TODO PERF this is a naive implementation, if this is performance critical we may want to optimize
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

        public bool Expandable => false;

        public void VerifyAlignment()
        {
            // TODO Olivier: port if required
            throw new NotImplementedException();
        }

        ///////////////////////////////////////////////////////////////////////////

        public long GetLong(int index, ByteOrder byteOrder)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            var value = *(long*)(_pBuffer + index);
            return EndianessConverter.ApplyInt64(byteOrder, value);
        }

        public void PutLong(int index, long value, ByteOrder byteOrder)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);
            
            value = EndianessConverter.ApplyInt64(byteOrder, value);
            *(long*)(_pBuffer + index) = value;
        }

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

            return Thread.VolatileRead(ref *(long*) (_pBuffer + index));
        }

        public void PutLongVolatile(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            Thread.VolatileWrite(ref *(long*) (_pBuffer + index), value);
            //UNSAFE.PutLongVolatile(byteArray, addressOffset + index, value);
        }

        public void PutLongOrdered(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            Thread.VolatileWrite(ref *(long*) (_pBuffer + index), value);
            //UNSAFE.PutOrderedLong(byteArray, addressOffset + index, value);
        }

        public long AddLongOrdered(int index, long increment)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            //JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
            //ORIGINAL LINE: final long offset = addressOffset + index;
            long offset = addressOffset + index;
            //JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
            //ORIGINAL LINE: final byte[] byteArray = this.byteArray;
            sbyte[] byteArray = this.byteArray;
            //JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
            //ORIGINAL LINE: final long value = UNSAFE.getLong(byteArray, offset);
            long value = UNSAFE.GetLong(byteArray, offset);
            UNSAFE.PutOrderedLong(byteArray, offset, value + increment);

            return value;
        }

        public bool CompareAndSetLong(int index, long expectedValue, long updateValue)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return UNSAFE.CompareAndSwapLong(byteArray, addressOffset + index, expectedValue, updateValue);
        }

        public long GetAndSetLong(int index, long value)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return UNSAFE.GetAndSetLong(byteArray, addressOffset + index, value);
        }

        public long GetAndAddLong(int index, long delta)
        {
            BoundsCheck0(index, BitUtil.SizeOfLong);

            return UNSAFE.GetAndAddLong(byteArray, addressOffset + index, delta);
        }

        ///////////////////////////////////////////////////////////////////////////




        ///////////////////////////////////////////////////////////////////////////

        private void BoundsCheck(int index)
        {
            if (index < 0 || index >= _capacity)
            {
                throw new IndexOutOfRangeException($"index={index:D}, capacity={_capacity:D}");
            }
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
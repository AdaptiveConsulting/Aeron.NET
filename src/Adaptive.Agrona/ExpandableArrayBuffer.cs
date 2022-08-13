using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Expandable <seealso cref="IMutableDirectBuffer"/> that is backed by an array. When values are put into the buffer beyond its
    /// current length, then it will be expanded to accommodate the resulting position for the value.
    /// <para>
    /// Put operations will expand the capacity as necessary up to <seealso cref="MAX_ARRAY_LENGTH"/>. Get operations will throw
    /// a <seealso cref="System.IndexOutOfRangeException"/> if past current capacity.
    /// </para>
    /// <para>
    /// <b>Note:</b> this class has a natural ordering that is inconsistent with equals.
    /// Types may be different but equal on buffer contents.
    /// </para>
    /// </summary>
    public unsafe class ExpandableArrayBuffer : IMutableDirectBuffer
    {
        /// <summary>
        /// Maximum length to which the underlying buffer can grow. Some JVMs store state in the last few bytes.
        /// </summary>
        public static readonly int MAX_ARRAY_LENGTH = int.MaxValue - 8;

        /// <summary>
        /// Initial capacity of the buffer from which it will expand as necessary.
        /// </summary>
        public const int INITIAL_CAPACITY = 128;

        private byte[] _byteArray;
        private GCHandle _pinnedGcHandle;
        private byte* _pBuffer;

        /// <summary>
        /// Create an <seealso cref="ExpandableArrayBuffer"/> with an initial length of <seealso cref="INITIAL_CAPACITY"/>.
        /// </summary>
        public ExpandableArrayBuffer() : this(INITIAL_CAPACITY)
        {
        }

        /// <summary>
        /// Create an <seealso cref="ExpandableArrayBuffer"/> with a provided initial length.
        /// </summary>
        /// <param name="initialCapacity"> of the buffer. </param>
        public ExpandableArrayBuffer(int initialCapacity)
        {
            AllocateAndPinArray(initialCapacity);
        }

        private void AllocateAndPinArray(int capacity)
        {
            _byteArray = new byte[capacity];
            // pin the buffer so it does not get moved around by GC, this is required since we use pointers
            _pinnedGcHandle = GCHandle.Alloc(_byteArray, GCHandleType.Pinned);
            _pBuffer = (byte*)_pinnedGcHandle.AddrOfPinnedObject().ToPointer();
        }

        /// <summary>
        /// Destructor for <see cref="ExpandableArrayBuffer"/>
        /// </summary>
        ~ExpandableArrayBuffer()
        {
            if (_pinnedGcHandle.IsAllocated)
            {
                _pinnedGcHandle.Free();
            }
        }

        public void Wrap(byte[] buffer)
        {
            throw new NotSupportedException();
        }

        public void Wrap(byte[] buffer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IDirectBuffer buffer)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IDirectBuffer buffer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IntPtr pointer, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IntPtr pointer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public int CompareTo(IDirectBuffer other)
        {
            throw new NotSupportedException();
        }

        public IntPtr BufferPointer => new IntPtr(_pBuffer);
        public byte[] ByteArray => _byteArray;
        public ByteBuffer ByteBuffer => null;
        public int Capacity => _byteArray.Length;

        public void CheckLimit(int limit)
        {
            EnsureCapacity(limit, BitUtil.SIZE_OF_BYTE);
        }

        /// <inheritdoc />
        public long GetLong(int index, ByteOrder byteOrder)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            var value = *(long*)(_pBuffer + index);
            return EndianessConverter.ApplyInt64(byteOrder, value);
        }

        /// <inheritdoc />
        public long GetLong(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);

            return *(long*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public int GetInt(int index, ByteOrder byteOrder)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            var value = *(int*)(_pBuffer + index);
            return EndianessConverter.ApplyInt32(byteOrder, value);
        }

        /// <inheritdoc />
        public int GetInt(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_INT);

            return *(int*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public double GetDouble(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_DOUBLE);

            return *(double*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public float GetFloat(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_FLOAT);

            return *(float*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public short GetShort(int index, ByteOrder byteOrder)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            var value = *(short*)(_pBuffer + index);
            return EndianessConverter.ApplyInt16(byteOrder, value);
        }

        /// <inheritdoc />
        public short GetShort(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_SHORT);

            return *(short*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public char GetChar(int index)
        {
            BoundsCheck0(index, BitUtil.SIZE_OF_CHAR);

            return *(char*)(_pBuffer + index);
        }

        /// <inheritdoc />
        public byte GetByte(int index)
        {
            return _byteArray[index];
        }

        /// <inheritdoc />
        public void GetBytes(int index, byte[] dst)
        {
            Array.Copy(_byteArray, index, dst, 0, dst.Length);
        }

        /// <inheritdoc />
        public void GetBytes(int index, byte[] dst, int offset, int length)
        {
            Array.Copy(_byteArray, index, dst, offset, length);
        }

        /// <inheritdoc />
        public void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length)
        {
            dstBuffer.PutBytes(dstIndex, _byteArray, index, length);
        }

        /// <inheritdoc />
        public string GetStringUtf8(int index)
        {
            var length = GetInt(index);

            return GetStringUtf8(index, length);
        }

        /// <inheritdoc />
        public string GetStringAscii(int index)
        {
            var length = GetInt(index);

            return GetStringAscii(index, length);
        }

        /// <inheritdoc />
        public int GetStringAscii(int index, StringBuilder appendable)
        {
            var length = GetInt(index);

            return GetStringAscii(index, length, appendable);
        }

        /// <inheritdoc />
        public int GetStringAscii(int index, int length, StringBuilder appendable)
        {
            for (int i = index + BitUtil.SIZE_OF_INT, limit = index + BitUtil.SIZE_OF_INT + length; i < limit; i++)
            {
                char c = *(char*)(_pBuffer + index);
                appendable.Append(c > (char)127 ? '?' : c);
            }

            return length;
        }

        /// <inheritdoc />
        public string GetStringUtf8(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SIZE_OF_INT, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        /// <inheritdoc />
        public string GetStringAscii(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index + BitUtil.SIZE_OF_INT, stringInBytes);

            return Encoding.ASCII.GetString(stringInBytes);
        }

        /// <inheritdoc />
        public string GetStringWithoutLengthUtf8(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index, stringInBytes);

            return Encoding.UTF8.GetString(stringInBytes);
        }

        /// <inheritdoc />
        public string GetStringWithoutLengthAscii(int index, int length)
        {
            var stringInBytes = new byte[length];
            GetBytes(index, stringInBytes);

            return Encoding.ASCII.GetString(stringInBytes);
        }

        /// <inheritdoc />
        public void BoundsCheck(int index, int length)
        {
            BoundsCheck0(index, length);
        }

        public bool IsExpandable => true;

        /// <inheritdoc />
        public void SetMemory(int index, int length, byte value)
        {
            EnsureCapacity(index, length);
            Unsafe.InitBlock(_pBuffer + index, value, (uint)length);
        }

        /// <inheritdoc />
        public void PutLong(int index, long value, ByteOrder byteOrder)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_LONG);

            value = EndianessConverter.ApplyInt64(byteOrder, value);
            *(long*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutLong(int index, long value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_LONG);

            BoundsCheck0(index, BitUtil.SIZE_OF_LONG);
            *(long*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutInt(int index, int value, ByteOrder byteOrder)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_INT);

            value = EndianessConverter.ApplyInt32(byteOrder, value);
            *(int*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutInt(int index, int value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_INT);

            *(int*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutDouble(int index, double value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_DOUBLE);

            *(double*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutFloat(int index, float value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_FLOAT);

            *(float*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutShort(int index, short value, ByteOrder byteOrder)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_SHORT);

            value = EndianessConverter.ApplyInt16(byteOrder, value);
            *(short*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutShort(int index, short value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_SHORT);

            *(short*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutChar(int index, char value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_CHAR);

            *(char*)(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutByte(int index, byte value)
        {
            EnsureCapacity(index, BitUtil.SIZE_OF_BYTE);

            *(_pBuffer + index) = value;
        }

        /// <inheritdoc />
        public void PutBytes(int index, byte[] src)
        {
            PutBytes(index, src, 0, src.Length);
        }

        /// <inheritdoc />
        public void PutBytes(int index, byte[] src, int offset, int length)
        {
            EnsureCapacity(index, length);
            Array.Copy(src, offset, _byteArray, index, length);
        }

        /// <inheritdoc />
        public void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            if (length == 0)
            {
                return;
            }

            EnsureCapacity(index, length);
            srcBuffer.BoundsCheck(srcIndex, length);

            byte* destination = _pBuffer + index;
            byte* source = (byte*)srcBuffer.BufferPointer.ToPointer() + srcIndex;
            ByteUtil.MemoryCopy(destination, source, (uint)length);
        }

        /// <inheritdoc />
        public int PutStringUtf8(int index, string value)
        {
            return PutStringUtf8(index, value, int.MaxValue);
        }

        /// <inheritdoc />
        public int PutStringAscii(int index, string value)
        {
            return PutStringAscii(index, value, int.MaxValue);
        }

        public int PutStringAscii(int index, string value, int maxEncodedSize)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.ASCII.GetBytes(value);
            if (bytes.Length > maxEncodedSize)
            {
                ThrowHelper.ThrowArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
            }

            EnsureCapacity(index, BitUtil.SIZE_OF_INT + bytes.Length);

            PutInt(index, bytes.Length);
            PutBytes(index + BitUtil.SIZE_OF_INT, bytes);

            return BitUtil.SIZE_OF_INT + bytes.Length;
        }


        /// <inheritdoc />
        public int PutStringWithoutLengthAscii(int index, string value)
        {
            int length = value?.Length ?? 0;

            EnsureCapacity(index, length);

            for (int i = 0; i < length; i++)
            {
                var c = value[i];
                if (c > (char)127)
                {
                    c = '?';
                }

                *(char*)(_pBuffer + index + i) = c;
            }

            return length;
        }

        /// <inheritdoc />
        public int PutStringWithoutLengthAscii(int index, string value, int valueOffset, int length)
        {
            var len = value != null ? Math.Min(value.Length - valueOffset, length) : 0;

            EnsureCapacity(index, len);

            for (int i = 0; i < len; i++)
            {
                char c = value[valueOffset + i];
                if (c > (char)127)
                {
                    c = '?';
                }

                *(char*)(_pBuffer + index + i) = c;
            }

            return len;
        }

        /// <inheritdoc />
        public int PutStringUtf8(int index, string value, int maxEncodedSize)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);
            if (bytes.Length > maxEncodedSize)
            {
                ThrowHelper.ThrowArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
            }

            EnsureCapacity(index, BitUtil.SIZE_OF_INT + bytes.Length);

            PutInt(index, bytes.Length);
            PutBytes(index + BitUtil.SIZE_OF_INT, bytes);

            return BitUtil.SIZE_OF_INT + bytes.Length;
        }

        /// <inheritdoc />
        public int PutStringWithoutLengthUtf8(int index, string value)
        {
            var bytes = value == null
                ? BufferUtil.NullBytes
                : Encoding.UTF8.GetBytes(value);

            EnsureCapacity(index, bytes.Length);
            PutBytes(index, bytes);

            return bytes.Length;
        }

        private void EnsureCapacity(int index, int length)
        {
            if (index < 0 || length < 0)
            {
                throw new IndexOutOfRangeException("negative value: index=" + index + " length=" + length);
            }

            long resultingPosition = index + (long)length;
            int currentArrayLength = _byteArray.Length;
            if (resultingPosition > currentArrayLength)
            {
                if (resultingPosition > MAX_ARRAY_LENGTH)
                {
                    throw new IndexOutOfRangeException("index=" + index + " length=" + length + " maxCapacity=" +
                                                       MAX_ARRAY_LENGTH);
                }

                int newLength = CalculateExpansion(currentArrayLength, resultingPosition);
                var oldArray = _byteArray;
                _pinnedGcHandle.Free();
                AllocateAndPinArray(newLength);
                Array.Copy(oldArray, _byteArray, oldArray.Length);
            }
        }

        private int CalculateExpansion(int currentLength, long requiredLength)
        {
            long value = Math.Max(currentLength, INITIAL_CAPACITY);

            while (value < requiredLength)
            {
                value += value >> 1;

                if (value > MAX_ARRAY_LENGTH)
                {
                    value = MAX_ARRAY_LENGTH;
                }
            }

            return (int)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BoundsCheck0(int index, int length)
        {
            int currentArrayLength = _byteArray.Length;
            long resultingPosition = index + (long)length;
            if (index < 0 || length < 0 || resultingPosition > currentArrayLength)
            {
                ThrowHelper.ThrowIndexOutOfRangeException(
                    $"index={index:D}, length={length:D}, capacity={Capacity:D}");
            }
        }
    }
}
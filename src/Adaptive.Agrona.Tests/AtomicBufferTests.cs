﻿using System;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests
{
    [TestFixture]
    public class AtomicBufferTests
    {
        private const int BufferCapacity = 4096;
        private const int Index = 8;

        private const byte ByteValue = 1;
        private const short ShortValue = byte.MaxValue + 2;
        private const char CharValue = '8';
        private const int IntValue = short.MaxValue + 3;
        private const float FloatValue = short.MaxValue + 4.0f;
        private const long LongValue = int.MaxValue + 5L;
        private const double DoubleValue = int.MaxValue + 7.0d;

        [Datapoint] public readonly UnsafeBuffer ByteArrayBacked = new UnsafeBuffer(new byte[BufferCapacity], 0, BufferCapacity);

        [Datapoint] public static readonly UnsafeBuffer UnmanagedBacked = new UnsafeBuffer(Marshal.AllocHGlobal(BufferCapacity), BufferCapacity);

        private static readonly MappedByteBuffer MappedByteBuffer = new MappedByteBuffer(MemoryMappedFile.CreateNew("testmap", BufferCapacity));

        [Datapoint] public static readonly UnsafeBuffer MemoryMappedFileBacked = new UnsafeBuffer(MappedByteBuffer.Pointer, BufferCapacity);

        [Theory]
        public void ShouldGetCapacity(UnsafeBuffer buffer)
        {
            Assert.That(buffer.Capacity, Is.EqualTo(BufferCapacity));
        }

        [Theory]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void ShouldThrowExceptionForAboveCapacity(UnsafeBuffer buffer)
        {
            var index = BufferCapacity + 1;
            buffer.CheckLimit(index);
        }

#if DEBUG
        [Theory]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void ShouldThrowExceptionWhenOutOfBounds(UnsafeBuffer buffer)
        {
            const int index = BufferCapacity;
            buffer.GetByte(index);
        }
#endif

        [Test]
        public void SharedBuffer()
        {
            var bb = new byte[1024];
            var ub1 = new UnsafeBuffer(bb, 0, 512);
            var ub2 = new UnsafeBuffer(bb, 512, 512);
            ub1.PutLong(Index, LongValue);
            ub2.PutLong(Index, 9876543210L);

            Assert.That(ub1.GetLong(Index), Is.EqualTo(LongValue));
        }

        [Test]
        public void ShouldVerifyBufferAlignment()
        {
            var buffer = new UnsafeBuffer(new byte[1024]);
            try
            {
                buffer.VerifyAlignment();
            }
            catch (InvalidOperationException ex)
            {
                Assert.Fail("All buffers should be aligned " + ex);
            }
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void ShouldThrowExceptionWhenBufferNotAligned()
        {
            var buffer = new UnsafeBuffer(new byte[1024], 1, 1023);
            buffer.VerifyAlignment();
        }

        [Theory]
        public void ShouldCopyMemory(UnsafeBuffer buffer)
        {
            var testBytes = Encoding.UTF8.GetBytes("xxxxxxxxxxx");

            buffer.SetMemory(0, testBytes.Length, (byte) 'x');

            for (var i = 0; i < testBytes.Length; i++)
            {
                Assert.That(Marshal.ReadByte(buffer.BufferPointer, i), Is.EqualTo(testBytes[i]));
            }
        }

        [Theory]
        public void ShouldGetLongFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt64(buffer.BufferPointer, Index, LongValue);

            Assert.That(buffer.GetLong(Index), Is.EqualTo(LongValue));
        }

        [Theory]
        public void ShouldPutLongToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutLong(Index, LongValue);

            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(LongValue));
        }

        [Theory]
        public void ShouldGetLongVolatileFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt64(buffer.BufferPointer, Index, LongValue);

            Assert.That(buffer.GetLongVolatile(Index), Is.EqualTo(LongValue));
        }

        [Theory]
        public void ShouldPutLongVolatileToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutLongVolatile(Index, LongValue);

            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(LongValue));
        }

        [Theory]
        public void ShouldPutLongOrderedToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutLongOrdered(Index, LongValue);

            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(LongValue));
        }

        [Theory]
        public void ShouldAddLongOrderedToNativeBuffer(UnsafeBuffer buffer)
        {
            var initialValue = int.MaxValue + 7L;
            const long increment = 9L;
            buffer.PutLongOrdered(Index, initialValue);
            buffer.AddLongOrdered(Index, increment);

            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(initialValue + increment));
        }

        [Theory]
        public void ShouldCompareAndSetLongToNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt64(buffer.BufferPointer, Index, LongValue);

            Assert.True(buffer.CompareAndSetLong(Index, LongValue, LongValue + 1));
            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(LongValue + 1));
        }

        [Theory]
        public void ShouldGetAndAddLongToNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt64(buffer.BufferPointer, Index, LongValue);

            const long delta = 1;
            var beforeValue = buffer.GetAndAddLong(Index, delta);

            Assert.That(beforeValue, Is.EqualTo(LongValue));
            Assert.That(Marshal.ReadInt64(buffer.BufferPointer, Index), Is.EqualTo(LongValue + delta));
        }

        [Theory]
        public void ShouldGetIntFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt32(buffer.BufferPointer, Index, IntValue);

            Assert.That(buffer.GetInt(Index), Is.EqualTo(IntValue));
        }

        [Theory]
        public void ShouldPutIntToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutInt(Index, IntValue);

            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(IntValue));
        }

        [Theory]
        public void ShouldGetIntVolatileFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt32(buffer.BufferPointer, Index, IntValue);

            Assert.That(buffer.GetIntVolatile(Index), Is.EqualTo(IntValue));
        }

        [Theory]
        public void ShouldPutIntVolatileToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutIntVolatile(Index, IntValue);

            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(IntValue));
        }

        [Theory]
        public void ShouldPutIntOrderedToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutIntOrdered(Index, IntValue);

            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(IntValue));
        }

        [Theory]
        public void ShouldAddIntOrderedToNativeBuffer(UnsafeBuffer buffer)
        {
            const int initialValue = 7;
            const int increment = 9;
            buffer.PutIntOrdered(Index, initialValue);
            buffer.AddIntOrdered(Index, increment);

            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(initialValue + increment));
        }

        [Theory]
        public void ShouldCompareAndSetIntToNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt32(buffer.BufferPointer, Index, IntValue);

            Assert.True(buffer.CompareAndSetInt(Index, IntValue, IntValue + 1));

            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(IntValue + 1));
        }

        [Theory]
        public void ShouldGetAndAddIntToNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt32(buffer.BufferPointer, Index, IntValue);

            const int delta = 1;
            var beforeValue = buffer.GetAndAddInt(Index, delta);

            Assert.That(beforeValue, Is.EqualTo(IntValue));
            Assert.That(Marshal.ReadInt32(buffer.BufferPointer, Index), Is.EqualTo(IntValue + delta));
        }

        [Theory]
        public void ShouldGetShortFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt16(buffer.BufferPointer, Index, ShortValue);

            Assert.That(buffer.GetShort(Index), Is.EqualTo(ShortValue));
        }

        [Theory]
        public void ShouldPutShortToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutShort(Index, ShortValue);

            Assert.That(Marshal.ReadInt16(buffer.BufferPointer, Index), Is.EqualTo(ShortValue));
        }

        [Theory]
        public void ShouldGetShortVolatileFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt16(buffer.BufferPointer, Index, ShortValue);

            Assert.That(buffer.GetShortVolatile(Index), Is.EqualTo(ShortValue));
        }

        [Theory]
        public void ShouldPutShortVolatileToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutShortVolatile(Index, ShortValue);

            Assert.That(Marshal.ReadInt16(buffer.BufferPointer, Index), Is.EqualTo(ShortValue));
        }

        [Theory]
        public void ShouldGetCharFromNativeBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteInt16(buffer.BufferPointer, Index, CharValue);

            Assert.That(buffer.GetChar(Index), Is.EqualTo(CharValue));
        }

        [Theory]
        public void ShouldPutCharToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutChar(Index, CharValue);

            Assert.That(Marshal.ReadInt16(buffer.BufferPointer, Index), Is.EqualTo(CharValue));
        }

        [Theory]
        public void ShouldGetDoubleFromNativeBuffer(UnsafeBuffer buffer)
        {
            var asLong = BitConverter.ToInt64(BitConverter.GetBytes(DoubleValue), 0);

            Marshal.WriteInt64(buffer.BufferPointer, Index, asLong);

            Assert.That(buffer.GetDouble(Index), Is.EqualTo(DoubleValue));
        }

        [Theory]
        public void ShouldPutDoubleToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutDouble(Index, DoubleValue);

            var valueAsLong = Marshal.ReadInt64(buffer.BufferPointer, Index);
            var valueAsDouble = BitConverter.ToDouble(BitConverter.GetBytes(valueAsLong), 0);

            Assert.That(valueAsDouble, Is.EqualTo(DoubleValue));
        }

        [Theory]
        public void ShouldGetFloatFromNativeBuffer(UnsafeBuffer buffer)
        {
            var asInt = BitConverter.ToInt32(BitConverter.GetBytes(FloatValue), 0);

            Marshal.WriteInt32(buffer.BufferPointer, Index, asInt);

            Assert.That(buffer.GetFloat(Index), Is.EqualTo(FloatValue));
        }

        [Theory]
        public void ShouldPutFloatToNativeBuffer(UnsafeBuffer buffer)
        {
            buffer.PutFloat(Index, FloatValue);

            var valueAsInt = Marshal.ReadInt32(buffer.BufferPointer, Index);
            var valueAsFloat = BitConverter.ToSingle(BitConverter.GetBytes(valueAsInt), 0);

            Assert.That(valueAsFloat, Is.EqualTo(FloatValue));
        }

        [Theory]
        public void ShouldGetByteFromBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteByte(buffer.BufferPointer, Index, ByteValue);

            Assert.That(buffer.GetByte(Index), Is.EqualTo(ByteValue));
        }

        [Theory]
        public void ShouldPutByteToBuffer(UnsafeBuffer buffer)
        {
            buffer.PutByte(Index, ByteValue);

            Assert.That(Marshal.ReadByte(buffer.BufferPointer, Index), Is.EqualTo(ByteValue));
        }

        [Theory]
        public void ShouldGetByteVolatileFromBuffer(UnsafeBuffer buffer)
        {
            Marshal.WriteByte(buffer.BufferPointer, Index, ByteValue);

            Assert.That(buffer.GetByteVolatile(Index), Is.EqualTo(ByteValue));
        }

        [Theory]
        public void ShouldPutByteVolatileToBuffer(UnsafeBuffer buffer)
        {
            buffer.PutByteVolatile(Index, ByteValue);

            Assert.That(Marshal.ReadByte(buffer.BufferPointer, Index), Is.EqualTo(ByteValue));
        }

        [Theory]
        public void ShouldGetByteArrayFromBuffer(UnsafeBuffer buffer)
        {
            byte[] testArray = {(byte) 'H', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o'};

            var i = Index;
            foreach (var v in testArray)
            {
                buffer.PutByte(i, v);
                i += BitUtil.SIZE_OF_BYTE;
            }

            var result = new byte[testArray.Length];
            buffer.GetBytes(Index, result);

            Assert.That(result, Is.EqualTo(testArray));
        }

        [Theory]
        public void ShouldGetBytesFromBuffer(UnsafeBuffer buffer)
        {
            var testBytes = Encoding.UTF8.GetBytes("Hello World");
            for (var i = 0; i < testBytes.Length; i++)
            {
                Marshal.WriteByte(buffer.BufferPointer, Index + i, testBytes[i]);
            }

            var buff = new byte[testBytes.Length];
            buffer.GetBytes(Index, buff);

            Assert.That(buff, Is.EqualTo(testBytes));
        }

        [Theory]
        public void ShouldPutBytesToBuffer(UnsafeBuffer buffer)
        {
            var testBytes = Encoding.UTF8.GetBytes("Hello World");
            buffer.PutBytes(Index, testBytes);

            var buff = new byte[testBytes.Length];
            for (var i = 0; i < testBytes.Length; i++)
            {
                buff[i] = Marshal.ReadByte(buffer.BufferPointer, Index + i);
            }

            Assert.That(buff, Is.EqualTo(testBytes));
        }

        [Theory]
        public void ShouldPutBytesToAtomicBufferFromAtomicBuffer(UnsafeBuffer buffer)
        {
            var testBytes = Encoding.UTF8.GetBytes("Hello World");
            var srcUnsafeBuffer = new UnsafeBuffer(testBytes);

            buffer.PutBytes(Index, srcUnsafeBuffer, 0, testBytes.Length);

            var buff = new byte[testBytes.Length];
            for (var i = 0; i < testBytes.Length; i++)
            {
                buff[i] = Marshal.ReadByte(buffer.BufferPointer, Index + i);
            }

            Assert.That(buff, Is.EqualTo(testBytes));
        }

        [Theory]
        public void ShouldGetBytesIntoAtomicBufferFromAtomicBuffer(UnsafeBuffer buffer)
        {
            var testBytes = Encoding.UTF8.GetBytes("Hello World");
            var srcUnsafeBuffer = new UnsafeBuffer(testBytes);

            srcUnsafeBuffer.GetBytes(0, buffer, Index, testBytes.Length);

            var buff = new byte[testBytes.Length];
            for (var i = 0; i < testBytes.Length; i++)
            {
                buff[i] = Marshal.ReadByte(buffer.BufferPointer, Index + i);
            }

            Assert.That(buff, Is.EqualTo(testBytes));
        }
    }
}
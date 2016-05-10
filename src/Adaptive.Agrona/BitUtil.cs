using System;
using System.Text;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Miscellaneous useful functions for dealing with low level bits and bytes.
    /// </summary>
    public class BitUtil
    {
        /// <summary>
        /// Size of a byte in bytes
        /// </summary>
        public const int SIZE_OF_BYTE = 1;

        /// <summary>
        /// Size of a boolean in bytes
        /// </summary>
        public const int SIZE_OF_BOOLEAN= 1;

        /// <summary>
        /// Size of a char in bytes
        /// </summary>
        public const int SIZE_OF_CHAR = 2;

        /// <summary>
        /// Size of a short in bytes
        /// </summary>
        public const int SIZE_OF_SHORT = 2;

        /// <summary>
        /// Size of an int in bytes
        /// </summary>
        public const int SIZE_OF_INT = 4;

        /// <summary>
        /// Size of a a float in bytes
        /// </summary>
        public const int SIZE_OF_FLOAT = 4;

        /// <summary>
        /// Size of a long in bytes
        /// </summary>
        public const int SIZE_OF_LONG = 8;

        /// <summary>
        /// Size of a double in bytes
        /// </summary>
        public const int SIZE_OF_DOUBLE = 8;

        /// <summary>
        /// Length of the data blocks used by the CPU cache sub-system in bytes.
        /// </summary>
        public const int CACHE_LINE_LENGTH = 64;

        // TODO use Char instead of Byte?

        private static readonly byte[] HexDigitTable = {
            (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
            (byte) '8', (byte) '9', (byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
        };

        private static readonly byte[] FromHexDigitTable;

        static BitUtil()
        {
            FromHexDigitTable = new byte[128];
            FromHexDigitTable['0'] = 0x00;
            FromHexDigitTable['1'] = 0x01;
            FromHexDigitTable['2'] = 0x02;
            FromHexDigitTable['3'] = 0x03;
            FromHexDigitTable['4'] = 0x04;
            FromHexDigitTable['5'] = 0x05;
            FromHexDigitTable['6'] = 0x06;
            FromHexDigitTable['7'] = 0x07;
            FromHexDigitTable['8'] = 0x08;
            FromHexDigitTable['9'] = 0x09;
            FromHexDigitTable['a'] = 0x0a;
            FromHexDigitTable['A'] = 0x0a;
            FromHexDigitTable['b'] = 0x0b;
            FromHexDigitTable['B'] = 0x0b;
            FromHexDigitTable['c'] = 0x0c;
            FromHexDigitTable['C'] = 0x0c;
            FromHexDigitTable['d'] = 0x0d;
            FromHexDigitTable['D'] = 0x0d;
            FromHexDigitTable['e'] = 0x0e;
            FromHexDigitTable['E'] = 0x0e;
            FromHexDigitTable['f'] = 0x0f;
            FromHexDigitTable['F'] = 0x0f;
        }

        private const int LastDigitMask = 1;

	    private static readonly Encoding Utf8Encoding = Encoding.UTF8;

        /// <summary>
        /// Fast method of finding the next power of 2 greater than or equal to the supplied value.
        /// 
        /// If the value is &lt;= 0 then 1 will be returned.
        /// 
        /// This method is not suitable for <seealso cref="int.MinValue"/> or numbers greater than 2^30.
        /// </summary>
        /// <param name="value"> from which to search for next power of 2 </param>
        /// <returns> The next power of 2 or the value itself if it is a power of 2 </returns>
        public static int FindNextPositivePowerOfTwo(int value)
        {
            return 1 << (32 - IntUtil.NumberOfLeadingZeros(value - 1));
        }

        /// <summary>
        /// Align a value to the next multiple up of alignment.
        /// If the value equals an alignment multiple then it is returned unchanged.
        /// <para>
        /// This method executes without branching. This code is designed to be use in the fast path and should not
        /// be used with negative numbers. Negative numbers will result in undefined behaviour.
        /// 
        /// </para>
        /// </summary>
        /// <param name="value">     to be aligned up. </param>
        /// <param name="alignment"> to be used. </param>
        /// <returns> the value aligned to the next boundary. </returns>
        public static int Align(int value, int alignment)
        {
            return (value + (alignment - 1)) & ~(alignment - 1);
        }

        /// <summary>
        /// Generate a byte array from the hex representation of the given byte array.
        /// </summary>
        /// <param name="buffer"> to convert from a hex representation (in Big Endian) </param>
        /// <returns> new byte array that is decimal representation of the passed array </returns>
        public static byte[] FromHexByteArray(byte[] buffer)
        {
            byte[] outputBuffer = new byte[buffer.Length >> 1];

            for (int i = 0; i < buffer.Length; i += 2)
            {
                outputBuffer[i >> 1] = (byte)((FromHexDigitTable[buffer[i]] << 4) | FromHexDigitTable[buffer[i + 1]]);
            }

            return outputBuffer;
        }

        /// <summary>
        /// Generate a byte array that is a hex representation of a given byte array.
        /// </summary>
        /// <param name="buffer"> to convert to a hex representation </param>
        /// <returns> new byte array that is hex representation (in Big Endian) of the passed array </returns>
        public static byte[] ToHexByteArray(byte[] buffer)
        {
            return ToHexByteArray(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Generate a byte array that is a hex representation of a given byte array.
        /// </summary>
        /// <param name="buffer"> to convert to a hex representation </param>
        /// <param name="offset"> the offset into the buffer </param>
        /// <param name="length"> the number of bytes to convert </param>
        /// <returns> new byte array that is hex representation (in Big Endian) of the passed array </returns>
        public static byte[] ToHexByteArray(byte[] buffer, int offset, int length)
        {
            var outputBuffer = new byte[length << 1];

            for (var i = 0; i < (length << 1); i += 2)
            {
                var b = buffer[offset + (i >> 1)];

                outputBuffer[i] = HexDigitTable[(b >> 4) & 0x0F];
                outputBuffer[i + 1] = HexDigitTable[b & 0x0F];
            }

            return outputBuffer;
        }

        /// <summary>
        /// Generate a byte array from a string that is the hex representation of the given byte array.
        /// </summary>
        /// <param name="value"> to convert from a hex representation (in Big Endian) </param>
        /// <returns> new byte array holding the decimal representation of the passed array </returns>
        public static byte[] FromHex(string value)
        {
            return FromHexByteArray(Utf8Encoding.GetBytes(value));
        }

        /// <summary>
        /// Generate a string that is the hex representation of a given byte array.
        /// </summary>
        /// <param name="buffer"> to convert to a hex representation </param>
        /// <param name="offset"> the offset into the buffer </param>
        /// <param name="length"> the number of bytes to convert </param>
        /// <returns> new String holding the hex representation (in Big Endian) of the passed array </returns>
        public static string ToHex(byte[] buffer, int offset, int length)
        {
            var hexByteArray = ToHexByteArray(buffer, offset, length);
            return Utf8Encoding.GetString(hexByteArray, 0, hexByteArray.Length);
        }

        /// <summary>
        /// Generate a string that is the hex representation of a given byte array.
        /// </summary>
        /// <param name="buffer"> to convert to a hex representation </param>
        /// <returns> new String holding the hex representation (in Big Endian) of the passed array </returns>
        public static string ToHex(byte[] buffer)
        {
            var hexByteArray = ToHexByteArray(buffer);
            return Utf8Encoding.GetString(hexByteArray, 0, hexByteArray.Length);
        }

        /// <summary>
        /// Is a number even.
        /// </summary>
        /// <param name="value"> to check. </param>
        /// <returns> true if the number is even otherwise false. </returns>
        public static bool IsEven(int value)
        {
            return (value & LastDigitMask) == 0;
        }

        /// <summary>
        /// Is a value a positive power of two.
        /// </summary>
        /// <param name="value"> to be checked. </param>
        /// <returns> true if the number is a positive power of two otherwise false. </returns>
        public static bool IsPowerOfTwo(int value)
        {
            return value > 0 && ((value & (~value + 1)) == value);
        }

        /// <summary>
        /// Cycles indices of an array one at a time in a forward fashion
        /// </summary>
        /// <param name="current"> value to be incremented. </param>
        /// <param name="max">     value for the cycle. </param>
        /// <returns> the next value, or zero if max is reached. </returns>
        public static int Next(int current, int max)
        {
            int next = current + 1;
            if (next == max)
            {
                next = 0;
            }

            return next;
        }

        /// <summary>
        /// Cycles indices of an array one at a time in a backwards fashion
        /// </summary>
        /// <param name="current"> value to be decremented. </param>
        /// <param name="max">     value of the cycle. </param>
        /// <returns> the next value, or max - 1 if current is zero </returns>
        public static int Previous(int current, int max)
        {
            if (0 == current)
            {
                return max - 1;
            }

            return current - 1;
        }

        // Note olivier probably not needed in the .NET port`

        ///// <summary>
        ///// Calculate the shift value to scale a number based on how refs are compressed or not.
        ///// </summary>
        ///// <param name="scale"> of the number reported by Unsafe. </param>
        ///// <returns> how many times the number needs to be shifted to the left. </returns>
        //public static int CalculateShiftForScale(int scale)
        //{
        //    if (4 == scale)
        //    {
        //        return 2;
        //    }
        //    if (8 == scale)
        //    {
        //        return 3;
        //    }
        //    throw new ArgumentException("Unknown pointer size");
        //}



        /// <summary>
        /// Generate a randomized integer over [<seealso cref="int.MinValue"/>, <seealso cref="int.MaxValue"/>] suitable for
        /// use as an Aeron Id.
        /// </summary>
        /// <returns> randomized integer suitable as an Id. </returns>
        public static int GenerateRandomisedId()
        {
            // Note Olivier: I've not ported that yet as it uses a standard Java class under the hood which does not have a .NET equivalent.
            // we can port later, when required.
            throw new NotImplementedException();
            //return ThreadLocalRandom.Current().Next();
        }

        /// <summary>
        /// Is an address aligned on a boundary.
        /// </summary>
        /// <param name="address">   to be tested. </param>
        /// <param name="alignment"> boundary the address is tested against. </param>
        /// <returns> true if the address is on the aligned boundary otherwise false. </returns>
        /// <exception cref="ArgumentException"> if the alignment is not a power of 2` </exception>
        public static bool IsAligned(long address, int alignment)
        {
            if (!IsPowerOfTwo(alignment))
            {
                throw new ArgumentException("Alignment must be a power of 2: alignment=" + alignment);
            }

            return (address & (alignment - 1)) == 0;
        }
    }
}
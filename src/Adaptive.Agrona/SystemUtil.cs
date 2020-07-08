using System;

namespace Adaptive.Agrona
{
    public class SystemUtil
    {
        private const long MAX_G_VALUE = 8589934591L;
        private const long MAX_M_VALUE = 8796093022207L;
        private const long MAX_K_VALUE = 9007199254739968L;
        
        private const long SECONDS_TO_NANOS = 1000000000;
        private const long MILLS_TO_NANOS = 1000000;
        private const long MICROS_TO_NANOS = 1000;

        /// <summary>
        /// Parse a string representation of a value with optional suffix of 'g', 'm', and 'k' suffix to indicate
        /// gigabytes, megabytes, or kilobytes respectively.
        /// </summary>
        /// <param name="propertyName">  that associated with the size value. </param>
        /// <param name="propertyValue"> to be parsed. </param>
        /// <returns> the long value. </returns>
        /// <exception cref="FormatException"> if the value is out of range or mal-formatted. </exception>
        public static long ParseSize(string propertyName, string propertyValue)
        {
            int lengthMinusSuffix = propertyValue.Length - 1;
            char lastCharacter = propertyValue[lengthMinusSuffix];
            if (char.IsDigit(lastCharacter))
            {
                return long.Parse(propertyValue);
            }

            long value = Convert.ToInt64(propertyValue.Substring(0, lengthMinusSuffix));

            switch (lastCharacter)
            {
                case 'k':
                case 'K':
                    if (value > MAX_K_VALUE)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024;

                case 'm':
                case 'M':
                    if (value > MAX_M_VALUE)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024 * 1024;

                case 'g':
                case 'G':
                    if (value > MAX_G_VALUE)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024 * 1024 * 1024;

                default:
                    throw new FormatException(propertyName + ": " + propertyValue + " should end with: k, m, or g.");
            }
        }

        /// <summary>
        /// Parse a string representation of a time duration with an optional suffix of 's', 'ms', 'us', or 'ns' to
        /// indicate seconds, milliseconds, microseconds, or nanoseconds respectively.
        /// <para>
        /// If the resulting duration is greater than <seealso cref="long.MaxValue"/> then <seealso cref="long.MaxValue"/> is used.
        /// 
        /// </para>
        /// </summary>
        /// <param name="propertyName">  associated with the duration value. </param>
        /// <param name="propertyValue"> to be parsed. </param>
        /// <returns> the long value. </returns>
        /// <exception cref="FormatException"> if the value is negative or malformed. </exception>
        public static long ParseDuration(string propertyName, string propertyValue)
        {
            char lastCharacter = propertyValue[propertyValue.Length - 1];
            if (char.IsDigit(lastCharacter))
            {
                return long.Parse(propertyValue);
            }

            if (lastCharacter != 's' && lastCharacter != 'S')
            {
                throw new FormatException(propertyName + ": " + propertyValue + " should end with: s, ms, us, or ns.");
            }

            char secondLastCharacter = propertyValue[propertyValue.Length - 2];
            if (char.IsDigit(secondLastCharacter))
            {
                long value = Convert.ToInt64(propertyValue.Substring(0, propertyValue.Length - 1));
                return SECONDS_TO_NANOS * value;
            }
            else
            {
                long value = Convert.ToInt64(propertyValue.Substring(0, propertyValue.Length - 2));

                switch (secondLastCharacter)
                {
                    case 'n':
                    case 'N':
                        return value;

                    case 'u':
                    case 'U':
                        return MICROS_TO_NANOS * value;

                    case 'm':
                    case 'M':
                        return MILLS_TO_NANOS * value;

                    default:
                        throw new FormatException(propertyName + ": " + propertyValue +
                                                         " should end with: s, ms, us, or ns.");
                }
            }
        }
    }
}
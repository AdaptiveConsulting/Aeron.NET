/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

namespace Adaptive.Agrona
{
    public class TimeUnit
    {
        public static readonly TimeUnit NANOSECONDS = new TimeUnit();
        public static readonly TimeUnit MILLIS = new TimeUnit();

        private TimeUnit() { }

        public long Convert(long sourceValue, TimeUnit destinationTimeUnit)
        {
            if (destinationTimeUnit == NANOSECONDS)
            {
                if (this == MILLIS)
                {
                    return sourceValue * 1000000;
                }

                if (this == NANOSECONDS)
                {
                    return sourceValue;
                }
            }

            if (destinationTimeUnit == MILLIS)
            {
                if (this == MILLIS)
                {
                    return sourceValue;
                }

                if (this == NANOSECONDS)
                {
                    return sourceValue / 1000000;
                }
            }

            throw new ArgumentException(
                $"Unsupported conversion from {this} to {destinationTimeUnit}",
                nameof(destinationTimeUnit)
            );
        }

        public long ToMillis(long value)
        {
            return Convert(value, MILLIS);
        }

        public long ToNanos(long value)
        {
            return Convert(value, NANOSECONDS);
        }
    }
}

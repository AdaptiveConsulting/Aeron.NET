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

using System.Threading;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona.Concurrent
{
    public class ControllableIdleStrategy : IIdleStrategy
    {
        public const int NOT_CONTROLLED = 0;
        public const int NOOP = 1;
        public const int BUSY_SPIN = 2;
        public const int YIELD = 3;
        public const int PARK = 4;

        private const long ParkPeriodNanoseconds = 1000;

        private readonly StatusIndicatorReader _statusIndicatorReader;

        public ControllableIdleStrategy(StatusIndicatorReader statusIndicatorReader)
        {
            _statusIndicatorReader = statusIndicatorReader;
        }

        /// <summary>
        /// Idle based on current status indication value
        /// </summary>
        /// <param name="workCount"> performed in last duty cycle. </param>
        /// <seealso cref="IIdleStrategy.Idle(int)"></seealso>
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Idle();
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public void Idle()
        {
            int status = (int)_statusIndicatorReader.GetVolatile();

            switch (status)
            {
                case NOOP:
                    break;

                case BUSY_SPIN:
                    Thread.SpinWait(0);
                    break;

                case YIELD:
                    Thread.Yield();
                    break;

                default:
                    LockSupport.ParkNanos(ParkPeriodNanoseconds);
                    break;
            }
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public void Reset()
        {
        }

        public override string ToString()
        {
            return "ControllableIdleStrategy{" + "statusIndicatorReader=" + _statusIndicatorReader + '}';
        }
    }
}

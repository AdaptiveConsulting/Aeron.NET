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
using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Idling strategy for threads when they have no work to do.
    /// 
    /// Spin for maxSpins, then
    /// <see cref="Thread.Yield"/> for maxYields, then
    /// <see cref="Thread.Sleep(int)"/> on an exponential backup to maxParkPeriodMs
    /// </summary>
    /// TODO Padding to avoid false sharing
    public class BackoffIdleStrategy : IIdleStrategy
    {
        private enum State
        {
            NOT_IDLE,
            SPINNING,
            YIELDING,
            PARKING
        }

        private readonly long _maxSpins;
        private readonly long _maxYields;
        private readonly int _minParkPeriodMs;
        private readonly int _maxParkPeriodMs;

        private State _state;

        private long _spins;
        private long _yields;
        private int _parkPeriodMs;

        /// <summary>
        /// Create a set of state tracking idle behavior
        /// </summary>
        /// <param name="maxSpins"> to perform before moving to <see cref="Thread.Yield"/></param>
        /// <param name="maxYields"> to perform before moving to <see cref="Thread.Sleep(int)"/></param>
        /// <param name="minParkPeriodMs"> to use when initating parkiing</param>
        /// <param name="maxParkPeriodMs"> to use when parking</param>
        public BackoffIdleStrategy(long maxSpins, long maxYields, long minParkPeriodMs, long maxParkPeriodMs)
        {
            _maxSpins = maxSpins;
            _maxYields = maxYields;
            _minParkPeriodMs = (int)minParkPeriodMs;
            _maxParkPeriodMs = (int)maxParkPeriodMs;
            _state = State.NOT_IDLE;
        }

        /// <inheritdoc />
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                Reset();
            }
            else
            {
                Idle();
            }
        }

        /// <inheritdoc />
        public void Idle()
        {
            switch (_state)
            {
                case State.NOT_IDLE:
                    _state = State.SPINNING;
                    _spins++;
                    break;

                case State.SPINNING:
                    if (++_spins > _maxSpins)
                    {
                        _state = State.YIELDING;
                        _yields = 0;
                    }
                    break;

                case State.YIELDING:
                    if (++_yields > _maxYields)
                    {
                        _state = State.PARKING;
                        _parkPeriodMs = _minParkPeriodMs;
                    }
                    else
                    {
                        Thread.Yield();
                    }
                    break;

                case State.PARKING:
                    Thread.Sleep(_parkPeriodMs);
                    _parkPeriodMs = Math.Min(_parkPeriodMs << 1, _maxParkPeriodMs);
                    break;
            }
        }

        /// <inheritdoc />
        public void Reset()
        {
            _spins = 0;
            _yields = 0;
            _state = State.NOT_IDLE;
        }
    }
}
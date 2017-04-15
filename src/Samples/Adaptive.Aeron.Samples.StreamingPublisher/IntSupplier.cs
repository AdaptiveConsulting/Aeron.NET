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
using Adaptive.Agrona;

namespace Adaptive.Aeron.Samples.StreamingPublisher
{
    internal class IntSupplier
    {
        private readonly ThreadLocal<Random> _random = new ThreadLocal<Random>(() => new Random());
        private readonly int _max;
        private readonly bool _isRandom;

        public IntSupplier(bool random, int max)
        {
            _isRandom = random;
            _max = max;
        }

        public int AsInt
        {
            get
            {
                if (_isRandom)
                {
                    return _random.Value.Next(BitUtil.SIZE_OF_LONG, _max);
                }

                return _max;
            }
        }
    }
}
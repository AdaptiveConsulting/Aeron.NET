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

using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// <see cref="IIdleStrategy"/> which uses a <see cref="SpinWait"/>.
    /// </summary>
    public class SpinWaitIdleStrategy : IIdleStrategy
    {
        private SpinWait _spinWait = new SpinWait();

        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            _spinWait.SpinOnce();
        }

        public void Idle()
        {
            _spinWait.SpinOnce();
        }

        public void Reset()
        {
            _spinWait.Reset();
        }
    }
}
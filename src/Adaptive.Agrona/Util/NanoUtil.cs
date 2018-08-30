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

namespace Adaptive.Agrona.Util
{
    public static class NanoUtil
    {
        public static long FromSeconds(long seconds)
        {
            return seconds*1000*1000*1000;
        }

        public static long FromMilliseconds(long milliseconds)
        {
            return milliseconds * 1000 * 1000;
        }

        public static int ToMillis(long resourceLingerDurationNs)
        {
            return (int)(resourceLingerDurationNs / (1000 * 1000));
        }
    }
}
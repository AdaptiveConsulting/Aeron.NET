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

using System.Diagnostics.CodeAnalysis;

namespace Adaptive.Agrona
{
    [SuppressMessage(
        "Performance",
        "CA1815",
        Justification = "Cache-line padding struct; equality semantics are deliberately unused."
    )]
    public struct CacheLinePadding
    {
#pragma warning disable 649
        private long _p1,
            _p2,
            _p3,
            _p4,
            _p5,
            _p6,
            _p7,
            _p8,
            _p9,
            _p10,
            _p11,
            _p12,
            _p13,
            _p14,
            _p15;
#pragma warning restore 649

        // To prevent compiler removing unused padding fields
        public override string ToString()
        {
            return $"{nameof(_p1)}: {_p1}, {nameof(_p2)}: {_p2}, {nameof(_p3)}: {_p3}, "
                + $"{nameof(_p4)}: {_p4}, {nameof(_p5)}: {_p5}, {nameof(_p6)}: {_p6}, "
                + $"{nameof(_p7)}: {_p7}, {nameof(_p8)}: {_p8}, {nameof(_p9)}: {_p9}, "
                + $"{nameof(_p10)}: {_p10}, {nameof(_p11)}: {_p11}, {nameof(_p12)}: {_p12}, "
                + $"{nameof(_p13)}: {_p13}, {nameof(_p14)}: {_p14}, {nameof(_p15)}: {_p15}";
        }
    }
}

﻿/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    using System.Collections.Generic;

    namespace io.aeron
    {
        public static class ListUtil
        {
            /// <summary>
            /// Removes element at index i, but instead of copying all elements to the left, moves into the same slot the last
            /// element. This avoids the copy costs, but spoils the list order. If i is the last element it is just removed.
            /// </summary>
            /// <param name="list">      to be modified.</param>
            /// <param name="i">         removal index.</param>
            /// <param name="lastIndex"> last element index.</param>
            /// <typeparam name="T">     element type.</typeparam>
            /// <exception cref="IndexOutOfRangeException"> if i or lastIndex are out of bounds.</exception>
            public static void FastUnorderedRemove<T>(List<T> list, int i, int lastIndex)
            {
                T last = list[lastIndex];
                list.RemoveAt(lastIndex);
                if (i != lastIndex)
                {
                    list[i] = last;
                }
            }

            /// <summary>
            /// Removes element but instead of copying all elements to the left, moves into the same slot the last element.
            /// This avoids the copy costs, but spoils the list order. If e is the last element it is just removed.
            /// </summary>
            /// <param name="list">      to be modified.</param>
            /// <param name="e">         to be removed.</param>
            /// <typeparam name="T">     element type.</typeparam>
            /// <returns> true if found and removed, false otherwise.</returns>
            public static bool FastUnorderedRemove<T>(List<T> list, T e)
            {
                if (e == null)
                {
                    ThrowHelper.ThrowArgumentNullException(nameof(e));
                }

                for (int i = 0, size = list.Count; i < size; i++)
                {
                    if (e.Equals(list[i]))
                    {
                        FastUnorderedRemove(list, i, size - 1);
                        return true;
                    }
                }

                return false;
            }
        }
    }
}
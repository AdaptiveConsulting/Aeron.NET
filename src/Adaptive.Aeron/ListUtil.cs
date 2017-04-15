namespace Adaptive.Aeron
{
    using System.Collections.Generic;

    namespace io.aeron
    {
        public abstract class ListUtil
        {
            private ListUtil()
            {
            }

            /// <summary>
            /// Removes element at i, but instead of copying all elements to the left, moves into the same slot the last
            /// element. If i is the last element it is just removed. This avoids the copy costs, but spoils the list order.
            /// </summary>
            /// <param name="list">      to be modified </param>
            /// <param name="i">         removal index </param>
            /// <param name="lastIndex"> last element index </param>
            public static void FastUnorderedRemove<T>(List<T> list, int i, int lastIndex)
            {
                T last = list[lastIndex];
                list.RemoveAt(lastIndex);
                if (i != lastIndex)
                {
                    list[i] = last;
                }
            }
        }
    }

}
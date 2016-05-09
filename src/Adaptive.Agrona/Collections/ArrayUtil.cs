namespace Adaptive.Agrona.Collections
{
    using System;

    /// <summary>
    /// Utility class for operating on arrays as if they were collections. This is useful for
    /// critical paths where operations like add and remove are seldom used, but iterating
    /// is common and checkcast and indirection are comparatively expensive.
    /// 
    /// In all cases the array being mutated is assumed to be full.
    /// 
    /// In all cases reference equality is used.
    /// </summary>
    public sealed class ArrayUtil
    {
        public const int UNKNOWN_INDEX = -1;

        /// <summary>
        /// Add an element to an array resulting in a new array.
        /// </summary>
        /// <param name="oldElements">  to have the new element added. </param>
        /// <param name="elementToAdd"> for the new array. </param>
        /// <returns> a new array that is one bigger and containing the new element at the end. </returns>
        public static T[] Add<T>(T[] oldElements, T elementToAdd)
        {
            var length = oldElements.Length;
            var newElements = CopyOf(oldElements, length + 1);
            newElements[length] = elementToAdd;

            return newElements;
        }

        /// <summary>
        /// Remove an element from an array resulting in a new array if the element was found otherwise the old array.
        /// 
        /// Returns its input parameter if the element to remove isn't a member.
        /// </summary>
        /// <param name="oldElements">     to have the element removed from. </param>
        /// <param name="elementToRemove"> being searched for by identity semantics. </param>
        /// <returns> a new array without the element if found otherwise the original array. </returns>
        public static T[] Remove<T>(T[] oldElements, T elementToRemove)
        {
            int length = oldElements.Length;
            int index = UNKNOWN_INDEX;

            for (int i = 0; i < length; i++)
            {
                if (oldElements[i].Equals(elementToRemove))
                {
                    index = i;
                }
            }

            return Remove(oldElements, index);
        }

        public static T[] Remove<T>(T[] oldElements, int index)
        {
            if (index == UNKNOWN_INDEX)
            {
                return oldElements;
            }

            int oldLength = oldElements.Length;
            int newLength = oldLength - 1;
            T[] newElements = NewArray(oldElements, newLength);

            for (int i = 0, j = 0; i < oldLength; i++)
            {
                if (index != i)
                {
                    newElements[j++] = oldElements[i];
                }
            }

            return newElements;
        }

        /// <summary>
        /// Allocate a new array of the same type as another array.
        /// </summary>
        /// <param name="oldElements"> on which the new array is based. </param>
        /// <param name="length">      of the new array. </param>
        /// <returns>            the new array of requested length. </returns>
        public static T[] NewArray<T>(T[] oldElements, int length)
        {
            return (T[])Array.CreateInstance(oldElements.GetType().GetElementType(), length);
        }

        /// <summary>
        /// Ensure an array has the required capacity. Resizing only if needed.
        /// </summary>
        /// <param name="oldElements">    to ensure that are long enough. </param>
        /// <param name="requiredLength"> to ensure. </param>
        /// <returns>               an array of the required length. </returns>
        public static T[] EnsureCapacity<T>(T[] oldElements, int requiredLength)
        {
            T[] result = oldElements;

            if (oldElements.Length < requiredLength)
            {
                result = CopyOf(oldElements, requiredLength);
            }

            return result;
        }

        internal static T[] CopyOf<T>(T[] original, int newLength)
        {
            T[] dest = new T[newLength];
            Array.Copy(original, 0, dest, 0, Math.Min(original.Length, newLength));
            return dest;
        }
    }
}
using System;
using System.Collections.Generic;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Extension methods that emulate Java's <c>Throwable.addSuppressed</c> / <c>getSuppressed</c>
    /// on .NET's <see cref="Exception"/> type via the <see cref="Exception.Data"/> dictionary.
    /// </summary>
    public static class SuppressedExceptions
    {
        private const string SuppressedKey = "Adaptive.Agrona.SuppressedExceptions";

        public static void AddSuppressed(this Exception primary, Exception suppressed)
        {
            if (primary == null || suppressed == null || ReferenceEquals(primary, suppressed))
            {
                return;
            }

            if (!(primary.Data[SuppressedKey] is List<Exception> list))
            {
                list = new List<Exception>();
                primary.Data[SuppressedKey] = list;
            }

            list.Add(suppressed);
        }

        public static IReadOnlyList<Exception> GetSuppressed(this Exception primary)
        {
            if (primary?.Data[SuppressedKey] is List<Exception> list)
            {
                return list;
            }

            return Array.Empty<Exception>();
        }
    }
}

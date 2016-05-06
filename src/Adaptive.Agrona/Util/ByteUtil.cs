using System.Reflection.Emit;

namespace Adaptive.Agrona.Util
{
    /// <summary>
    /// Utility to copy blocks of memory
    /// Uses the IL instruction Cpblk which is not available in C#
    /// </summary>
    public unsafe class ByteUtil
    {
        // TODO PERF Olivier: write some benchmarks, is this the way to go?

        public delegate void MemoryCopyDelegate(void* destination, void* source, uint length);

        public static readonly MemoryCopyDelegate MemoryCopy;

        static ByteUtil()
        {
            var dynamicMethod = new DynamicMethod
            (
                "MemoryCopy",
                typeof(void),
                new[] { typeof(void*), typeof(void*), typeof(uint) },
                typeof(ByteUtil)
            );

            var ilGenerator = dynamicMethod.GetILGenerator();

            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldarg_1);
            ilGenerator.Emit(OpCodes.Ldarg_2);

            ilGenerator.Emit(OpCodes.Cpblk);
            ilGenerator.Emit(OpCodes.Ret);

            MemoryCopy = (MemoryCopyDelegate)dynamicMethod.CreateDelegate(typeof(MemoryCopyDelegate));
        }
    }
}
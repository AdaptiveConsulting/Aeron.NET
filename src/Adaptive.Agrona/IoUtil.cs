using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    public class IoUtil
    {
        /// <summary>
        /// Check that file exists, open file, and return <seealso cref="MappedByteBuffer"/> for entire file
        /// </summary>
        /// <param name="path"> of the file to map </param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(string path)
        {
            return new MappedByteBuffer(OpenMemoryMappedFile(path));
        }

        /// <summary>
        /// Check that file exists and open file
        /// </summary>
        /// <param name="path"> of the file to map </param>
        /// <returns> <seealso cref="MemoryMappedFile"/> the file </returns>
        public static MemoryMappedFile OpenMemoryMappedFile(string path)
        {
            CheckFileExists(path);

            var f = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            return MemoryMappedFile.CreateFromFile(f, Guid.NewGuid().ToString(), 0, MemoryMappedFileAccess.ReadWrite, new MemoryMappedFileSecurity(), HandleInheritability.None, false);
        }

        /// <summary>
        /// Unmap a <seealso cref="MappedByteBuffer"/> without waiting for the next GC cycle.
        /// </summary>
        /// <param name="wrapper"> to be unmapped. </param>
        public static void Unmap(MappedByteBuffer wrapper)
        {
            wrapper?.Dispose();
        }

        /// <summary>
        /// Check that a file exists and throw an exception if not.
        /// </summary>
        /// <param name="path"> to check existence of. </param>
        public static void CheckFileExists(string path)
        {
            if (!File.Exists(path))
            {
                string msg = $"Missing file {path}";
                throw new InvalidOperationException(msg);
            }
        }

        /// <summary>
        /// Return the system property for java.io.tmpdir ensuring a '/' is at the end.
        /// </summary>
        /// <returns> tmp directory for the runtime </returns>
        public static string TmpDirName()
        {
            return Path.GetTempPath();
        }
    }
}
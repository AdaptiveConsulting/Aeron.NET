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
        /// <param name="path">         of the file to map </param>
        /// <param name="descriptionLabel"> to be associated for any exceptions </param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(string path, string descriptionLabel)
        {
            CheckFileExists(path, descriptionLabel);

            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open);

            return new MappedByteBuffer(mmf);
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
        /// <param name="name"> to associate for the exception </param>
        public static void CheckFileExists(string path, string name)
        {
            if (!File.Exists(path))
            {
                string msg = $"Missing file for {name}: {path}";
                throw new InvalidOperationException(msg);
            }
        }

        /// <summary>
        /// Return the system property for java.io.tmpdir ensuring a '/' is at the end.
        /// </summary>
        /// <returns> tmp directory for the runtime </returns>
        public static string TmpDirName()
        {
            string tmpDirName = Path.GetTempPath();
            if (!tmpDirName.EndsWith("/", StringComparison.Ordinal))
            {
                tmpDirName += "/";
            }

            return tmpDirName;
        }
    }
}
using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    public class IoUtil
    {
        private static int name = 0;
        /// <summary>
        /// Check that file exists, open file, and return <seealso cref="MappedByteBuffer"/> for entire file
        /// </summary>
        /// <param name="path">         of the file to map </param>
        /// <param name="descriptionLabel"> to be associated for any exceptions </param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(string path, string descriptionLabel)
        {
            CheckFileExists(path, descriptionLabel);

            var f = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            var mmf = MemoryMappedFile.CreateFromFile(f, "foo", 0, MemoryMappedFileAccess.ReadWrite, new MemoryMappedFileSecurity(), HandleInheritability.None, false);
            
            return new MappedByteBuffer(mmf);
        }

        public static MappedByteBuffer OpenExisting(string logFileName)
        {
            if( !File.Exists(logFileName)) throw new FileNotFoundException("Couldn't find memory map", logFileName);

            var f = new FileStream(logFileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            
            var mmf = MemoryMappedFile.CreateFromFile(f, "blah"+(++name), 0, MemoryMappedFileAccess.ReadWrite, new MemoryMappedFileSecurity(), HandleInheritability.None, false);

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
            return Path.GetTempPath();
        }
    }
}
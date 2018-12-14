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

using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona
{
    public enum MapMode
    {
        ReadOnly,
        ReadWrite
    }

    public class IoUtil
    {
        /// <summary>
        /// Check that file exists, open file, and return <seealso cref="MappedByteBuffer"/> for entire file
        /// </summary>
        /// <param name="path"> of the file to map </param>
        /// <param name="mapMode"></param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(string path, MapMode mapMode)
        {
            return new MappedByteBuffer(OpenMemoryMappedFile(path));
        }

        /// <summary>
        /// Check that file exists, open file, and return <seealso cref="MappedByteBuffer"/> for entire file
        /// </summary>
        /// <param name="path"> of the file to map </param>
        /// <param name="offset"></param>
        /// <param name="length"></param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(FileInfo path, long offset, long length)
        {
            return new MappedByteBuffer(OpenMemoryMappedFile(path.FullName), offset, length);
        }

        public static MappedByteBuffer MapNewFile(FileInfo cncFile, long length, bool fillWithZeros = true)
        {
            var fileAccess = FileAccess.ReadWrite;
            var fileShare = FileShare.ReadWrite | FileShare.Delete;
            var memoryMappedFileAccess = MemoryMappedFileAccess.ReadWrite;

            var f = new FileStream(cncFile.FullName, FileMode.CreateNew, fileAccess, fileShare);

#if NETFULL
            var mmFile = MemoryMappedFile.CreateFromFile(f, null, length, memoryMappedFileAccess, new MemoryMappedFileSecurity(), HandleInheritability.None, false);
#else
            var mmFile = MemoryMappedFile.CreateFromFile(f, null, length, memoryMappedFileAccess, HandleInheritability.None, false);
#endif
            
            var mappedByteBuffer = new MappedByteBuffer(mmFile, 0, length);

            if (fillWithZeros)
            {
                mappedByteBuffer.FillWithZeros();
            }

            return mappedByteBuffer;
        }


        public static MappedByteBuffer MapNewOrExistingFile(FileInfo cncFile, long length)
        {
            var fileAccess = FileAccess.ReadWrite;
            var fileShare = FileShare.ReadWrite | FileShare.Delete;
            var memoryMappedFileAccess = MemoryMappedFileAccess.ReadWrite;

            var f = new FileStream(cncFile.FullName, FileMode.OpenOrCreate, fileAccess, fileShare);

#if NETFULL
            var mmFile = MemoryMappedFile.CreateFromFile(f, null, length, memoryMappedFileAccess, new MemoryMappedFileSecurity(), HandleInheritability.None, false);
#else
            var mmFile = MemoryMappedFile.CreateFromFile(f, null, length, memoryMappedFileAccess, HandleInheritability.None, false);
#endif
            
            var mappedByteBuffer = new MappedByteBuffer(mmFile, 0, length);

            return mappedByteBuffer;
        }


        /// <summary>
        /// Check that file exists and open file
        /// </summary>
        /// <param name="path"> of the file to map </param>
        /// <returns> <seealso cref="MemoryMappedFile"/> the file </returns>
        public static MemoryMappedFile OpenMemoryMappedFile(string path)
        {
            // mapMode == MapMode.ReadOnly -> here for parity with the Java version but no affect, UnauthorisedAccessExceptions/IOExceptions thrown when trying to open file in Read mode, all files are opened ReadWrite

            CheckFileExists(path);

            var fileAccess = FileAccess.ReadWrite;
            var fileShare = FileShare.ReadWrite | FileShare.Delete;

            var f = new FileStream(path, FileMode.Open, fileAccess, fileShare);

            return OpenMemoryMappedFile(f);
        }

        private static MemoryMappedFile OpenMemoryMappedFile(FileStream f)
        {
            var memoryMappedFileAccess = MemoryMappedFileAccess.ReadWrite;
#if NETFULL
            return MemoryMappedFile.CreateFromFile(f, null, 0, memoryMappedFileAccess, new MemoryMappedFileSecurity(), HandleInheritability.None, false);
#else
            return MemoryMappedFile.CreateFromFile(f, null, 0, memoryMappedFileAccess, HandleInheritability.None,
                false);
#endif
        }

        /// <summary>
        /// Return MappedByteBuffer for entire file
        /// <para>
        /// The file itself will be closed, but the mapping will persist.
        /// 
        /// </para>
        /// </summary>
        /// <param name="fileStream">         of the file to map </param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(FileStream fileStream)
        {
            return new MappedByteBuffer(OpenMemoryMappedFile(fileStream));
        }


        /// <summary>
        /// Check that file exists, open file, and return MappedByteBuffer for entire file
        /// <para>
        /// The file itself will be closed, but the mapping will persist.
        /// 
        /// </para>
        /// </summary>
        /// <param name="location">         of the file to map </param>
        /// <param name="descriptionLabel"> to be associated for any exceptions </param>
        /// <returns> <seealso cref="MappedByteBuffer"/> for the file </returns>
        public static MappedByteBuffer MapExistingFile(FileInfo location, string descriptionLabel)
        {
            CheckFileExists(location, descriptionLabel);

            return new MappedByteBuffer(OpenMemoryMappedFile(location.FullName));
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
        /// <param name="file"> to check existence of. </param>
        /// <param name="name"> to associate for the exception </param>
        public static void CheckFileExists(FileInfo file, string name)
        {
            if (!file.Exists)
            {
                string msg = "Missing file for " + name + " : " + file.FullName;
                throw new InvalidOperationException(msg);
            }
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

        private static readonly bool IsOSX = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        /// <summary>
        /// Return the system property for java.io.tmpdir ensuring a '/' is at the end.
        /// </summary>
        /// <returns> tmp directory for the runtime </returns>
        public static string TmpDirName()
        {
            if (Environment.OSVersion.Platform == PlatformID.Unix && !IsOSX)
            {
                return @"/dev/shm";
            }

            return Path.GetTempPath();
        }

        public static void Delete(DirectoryInfo directory, bool b)
        {
            if (directory.Exists)
            {
                directory.Delete(true);
            }
        }

        public static void EnsureDirectoryExists(DirectoryInfo directory, string descriptionLabel)
        {
            if (!directory.Exists)
            {
                directory.Create();

                if (!directory.Exists)
                {
                    throw new ArgumentException("could not create " + descriptionLabel + " directory: " +
                                                directory.FullName);
                }
            }
        }
    }
}
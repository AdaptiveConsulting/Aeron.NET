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
using System.Diagnostics;
using System.Linq;
using System.Text;
using Adaptive.Agrona;
using HardwareInformation;
using HardwareInformation.Information.Cpu;

namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// A class that uses the System.Management APIS (WMI) to fetch the most 
    /// interesting attributes about the computer hardware we are running on.  
    /// Based on Vance Morrison's MeasureIt tool: http://blogs.msdn.com/b/vancem/archive/2009/02/06/measureit-update-tool-for-doing-microbenchmarks.aspx
    /// </summary>
    public class ComputerSpecifications
    {
        public readonly string OperatingSystem;
        public readonly string OperatingSystemVersion;
        public readonly string OperatingSystemServicePack;

        public readonly int NumberOfProcessors;
        public readonly string ProcessorName;
        public readonly string ProcessorDescription;
        public readonly int ProcessorClockSpeedMhz;

        public readonly ulong MemoryMBytes;
        public readonly ulong L1KBytes;
        public readonly ulong L2KBytes;
        public readonly uint NumberOfCores;
        public readonly int NumberOfLogicalProcessors;
        public readonly ulong L3KBytes;

        public ComputerSpecifications()
        {
            var info = MachineInformationGatherer.GatherInformation();
            
            MemoryMBytes = (ulong) info.RAMSticks.Select(i => (long) i.Capacity).Sum();
            NumberOfLogicalProcessors = Environment.ProcessorCount;

            OperatingSystem = info.OperatingSystem.Platform.ToString();
            OperatingSystemVersion = info.OperatingSystem.VersionString;
            OperatingSystemServicePack = info.OperatingSystem.ServicePack;

            NumberOfProcessors = Environment.ProcessorCount;

            ProcessorName = info.Cpu.Name;
            ProcessorDescription = info.Cpu.Vendor;
            ProcessorClockSpeedMhz = (int) info.Cpu.MaxClockSpeed;
            L1KBytes = info.Cpu.Caches.First(c => c.Level == Cache.CacheLevel.LEVEL1).Capacity;
            L2KBytes = info.Cpu.Caches.First(c => c.Level == Cache.CacheLevel.LEVEL2).Capacity;
            L3KBytes = info.Cpu.Caches.First(c => c.Level == Cache.CacheLevel.LEVEL3).Capacity;
            NumberOfCores = info.Cpu.PhysicalCores;
            ClrVersion = RuntimeInformation.GetClrVersion();
            Architecture = GetArchitecture();
            HasAttachedDebugger = Debugger.IsAttached;
            HasRyuJit = RuntimeInformation.HasRyuJit();
            Configuration = RuntimeInformation.GetConfiguration();
        }

        public string Configuration { get; }


        private static string GetArchitecture() => IntPtr.Size == 4 ? "32-bit" : "64-bit";

        public bool HasRyuJit { get; }

        public bool HasAttachedDebugger { get; }

        public object Architecture { get; }

        public string ClrVersion { get; }

        private string GetDebuggerFlag() => HasAttachedDebugger ? " [AttachedDebugger]" : "";

        public static void Dump()
        {
            Console.WriteLine(new ComputerSpecifications().ToString());
        }

        public bool IsHyperThreaded => NumberOfCores != NumberOfLogicalProcessors;
        private string GetJitFlag() => HasRyuJit ? " [RyuJIT]" : "";

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append("Operating System: ").AppendLine(OperatingSystem);
            builder.Append(" - Version: ").AppendLine(OperatingSystemVersion);
            builder.Append(" - SP: ").Append(OperatingSystemServicePack).AppendLine();
            builder.AppendLine();
            builder.Append("Number of Processors: ").Append(NumberOfProcessors).AppendLine();
            builder.Append(" - Name: ").AppendLine(ProcessorName);
            builder.Append(" - Description: ").AppendLine(ProcessorDescription);
            builder.Append(" - ClockSpeed: ").Append(ProcessorClockSpeedMhz).AppendLine("Mhz");
            builder.Append(" - # Cores: ").AppendLine(NumberOfCores.ToString());
            builder.Append(" - # Logical processors: ").AppendLine(NumberOfLogicalProcessors.ToString());
            builder.Append(" - Hyperthreading: ").AppendLine(IsHyperThreaded ? "ON" : "OFF");
            builder.AppendLine();
            builder.AppendLine(
                $"Memory: {MemoryMBytes}MB, L1Cache: {L1KBytes}KB, L2Cache: {L2KBytes}KB, L3Cache: {L3KBytes}KB");
            builder.AppendLine(
                $".NET Runtime: CLR={ClrVersion}, Arch={Architecture} {Configuration}{GetDebuggerFlag()}{GetJitFlag()}");

            if (Config.Params.Count > 0)
            {
                builder.AppendLine();
                builder.AppendLine("Flags");
                foreach (var kvp in Config.Params)
                {
                    builder.AppendLine($"- {kvp.Key}={kvp.Value}");
                }
            }

            return builder.ToString();
        }
    }
}
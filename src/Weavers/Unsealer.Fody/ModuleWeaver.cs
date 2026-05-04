using System.Collections.Generic;
using System.Linq;
using Fody;
using Mono.Cecil;

namespace Unsealer.Fody
{
    public class ModuleWeaver : BaseModuleWeaver
    {
        public override void Execute()
        {
            int unsealed = 0;

            foreach (var type in AllTypes(ModuleDefinition.Types))
            {
                // Class reference types only: skip structs (IsValueType), interfaces,
                // enums, and the implicit `<Module>` type.
                if (!type.IsClass) continue;
                if (type.IsValueType) continue;
                if (type.IsInterface) continue;
                if (!type.IsSealed) continue;

                // Skip compiler-generated types (lambda closures, anonymous types,
                // async state machines, etc.) — these are sealed for legitimate
                // reasons and shouldn't be subclassed by tests anyway.
                if (HasCompilerGeneratedAttribute(type)) continue;
                if (type.Name.Contains('<')) continue; // safety net for nested compiler-gen names

                // Delegate types MUST remain sealed — the CLR enforces this and
                // assembly-loading throws TypeLoadException otherwise.
                if (IsDelegate(type)) continue;

                // C# static classes are emitted as `abstract sealed` in IL.
                // Removing `sealed` would change their meaning (no longer static).
                if (type.IsAbstract) continue;

                type.IsSealed = false;
                unsealed++;
            }

            WriteMessage($"Unsealer.Fody: unsealed {unsealed} class types.", MessageImportance.Low);
        }

        private static bool HasCompilerGeneratedAttribute(TypeDefinition type)
        {
            return type.HasCustomAttributes &&
                   type.CustomAttributes.Any(a => a.AttributeType.FullName ==
                       "System.Runtime.CompilerServices.CompilerGeneratedAttribute");
        }

        private static bool IsDelegate(TypeDefinition type)
        {
            var baseTypeName = type.BaseType?.FullName;
            return baseTypeName == "System.MulticastDelegate" || baseTypeName == "System.Delegate";
        }

        public override IEnumerable<string> GetAssembliesForScanning()
        {
            yield return "netstandard";
            yield return "mscorlib";
        }

        private static IEnumerable<TypeDefinition> AllTypes(IEnumerable<TypeDefinition> types)
        {
            foreach (var type in types)
            {
                yield return type;
                if (type.HasNestedTypes)
                {
                    foreach (var nested in AllTypes(type.NestedTypes))
                    {
                        yield return nested;
                    }
                }
            }
        }

        public override bool ShouldCleanReference => true;
    }
}

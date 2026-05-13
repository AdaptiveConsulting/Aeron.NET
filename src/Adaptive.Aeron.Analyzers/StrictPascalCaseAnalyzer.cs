/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Adaptive.Aeron.Analyzers;

/// <summary>
/// Flags identifiers that violate the Microsoft .NET capitalization conventions
/// (https://learn.microsoft.com/dotnet/standard/design-guidelines/capitalization-conventions).
///
/// Rules enforced:
///  1. Acronyms of 3+ letters must be PascalCase (Xml, Sql, Html), not all-caps.
///  2. The abbreviation "ID" is treated as a word ("Id"), even though it's only
///     2 letters — this is an explicit MS carve-out.
///
/// Not enforced (per MS): 2-letter acronyms stay all-caps (IO, DB, UI, OK).
/// Roslyn's first-char-only pascal_case check misses both rules; this analyzer
/// closes the gap for non-API symbols (plus public/protected in test paths).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class StrictPascalCaseAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "AERON0002";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        title: "Identifier violates MS PascalCase capitalization rules",
        messageFormat:
            "Identifier '{0}' contains a {1}-character all-caps acronym; only " +
            "2-letter acronyms stay uppercase per MS conventions, and 'ID' is " +
            "cased as 'Id'",
        category: "Naming",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description:
            "Microsoft's .NET capitalization conventions require 3+ letter acronyms " +
            "to be PascalCase (Xml, Sql, Html). 2-letter acronyms stay all-caps " +
            "(IO, DB, UI). The abbreviation 'ID' is a special case treated as a " +
            "word ('Id'). Roslyn's built-in pascal_case rule only checks the first " +
            "character, so this analyzer closes the gap for non-API symbols.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(
            AnalyzeSymbol,
            SymbolKind.Field,
            SymbolKind.Method,
            SymbolKind.Property,
            SymbolKind.Event,
            SymbolKind.NamedType,
            SymbolKind.Parameter);
    }

    private static void AnalyzeSymbol(SymbolAnalysisContext context)
    {
        var symbol = context.Symbol;
        var name = symbol.Name;
        if (!TryFindViolation(name, out var acronymLen))
        {
            return;
        }

        if (symbol.Locations.Length == 0)
        {
            return;
        }

        var path = symbol.Locations[0].SourceTree?.FilePath ?? "";

        // Skip SBE codecs (auto-generated; Roslyn's generated_code=true does
        // not propagate to third-party analyzers).
        if (path.Contains("/Codecs/") || path.Contains("\\Codecs\\"))
        {
            return;
        }

        // Non-API only — EXCEPT in test code, where public/protected aren't
        // consumer API and aren't subject to Java upstream parity.
        if (!IsTestPath(path))
        {
            switch (symbol.DeclaredAccessibility)
            {
                case Accessibility.Private:
                case Accessibility.Internal:
                case Accessibility.ProtectedOrInternal:
                case Accessibility.ProtectedAndInternal:
                    break;
                default:
                    return;
            }
        }

        context.ReportDiagnostic(Diagnostic.Create(
            Rule, symbol.Locations[0], name, acronymLen));
    }

    /// <summary>
    /// Returns true if <paramref name="name"/> contains either:
    ///  • A run of consecutive uppercase letters that decomposes into a
    ///    3+-letter acronym (with optional trailing PascalCase word start), or
    ///  • The substring "ID" positioned as a standalone 2-letter acronym
    ///    (MS-special-cased to "Id").
    /// </summary>
    private static bool TryFindViolation(string name, out int acronymLen)
    {
        acronymLen = 0;

        // Check 1: 3+ letter acronyms.
        for (int i = 0; i < name.Length;)
        {
            if (!char.IsUpper(name[i]))
            {
                i++;
                continue;
            }

            int j = i;
            while (j < name.Length && char.IsUpper(name[j]))
            {
                j++;
            }

            int runLen = j - i;
            if (runLen >= 3)
            {
                // If the run is followed by a lowercase letter, the last
                // uppercase letter is the first character of a new PascalCase
                // word — so the acronym proper is (runLen - 1) letters long.
                // Otherwise the whole run is an acronym.
                bool lastStartsNewWord = j < name.Length && char.IsLower(name[j]);
                int actualAcronymLen = lastStartsNewWord ? runLen - 1 : runLen;
                if (actualAcronymLen >= 3)
                {
                    acronymLen = actualAcronymLen;
                    return true;
                }
            }

            i = j;
        }

        // Check 2: "ID" as a standalone 2-letter acronym (the MS special case).
        for (int i = 0; i + 1 < name.Length; i++)
        {
            if (name[i] != 'I' || name[i + 1] != 'D')
            {
                continue;
            }

            bool leftOk = i == 0 || char.IsLower(name[i - 1]);
            bool rightOk = i + 2 == name.Length
                           || char.IsUpper(name[i + 2])
                           || char.IsDigit(name[i + 2]);
            if (leftOk && rightOk)
            {
                acronymLen = 2;
                return true;
            }
        }

        return false;
    }

    private static bool IsTestPath(string path) =>
        path.Contains(".Tests/") || path.Contains(".Tests\\") ||
        path.Contains(".Test/") || path.Contains(".Test\\");
}

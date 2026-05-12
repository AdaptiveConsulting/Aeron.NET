; Unshipped analyzer release.
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
AERON0002 | Naming | Warning | MS PascalCase acronym detector. Catches 3+ letter all-caps acronyms (XMLParser -> XmlParser) and the MS-special-cased ID (StreamID -> StreamId). Allows 2-letter acronyms (IO, DB, UI) per MS conventions.

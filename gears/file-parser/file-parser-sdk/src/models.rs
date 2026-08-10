use bytes::Bytes;

/// Raw bytes to extract text from, plus the hints `file-parser` uses to pick
/// a backend (see `file_parser::domain::service::FileParserService::parse_bytes`).
#[derive(Debug, Clone)]
pub struct ParseBytesRequest {
    /// Original filename, if known — used to disambiguate the file type when
    /// `content_type` is absent or generic (e.g. `application/octet-stream`).
    pub filename: Option<String>,
    /// MIME type, if known.
    pub content_type: Option<String>,
    /// Raw file bytes.
    pub bytes: Bytes,
}

/// Extracted text for a parsed attachment, rendered as Markdown so it can be
/// scanned as plain text by consumers (e.g. `scan-gateway`'s detector
/// plugins, which only operate on `String` payloads) or injected into an
/// LLM prompt.
#[derive(Debug, Clone)]
pub struct ParsedText {
    /// Markdown rendering of the parsed document.
    pub markdown: String,
}

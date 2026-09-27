///|
/// Keep the derived trait methods that form part of the public API explicit.
pub extend TransportError with Eq::{not_equal, equal}

///|
pub extend TransportError with @debug.Debug::{to_repr}

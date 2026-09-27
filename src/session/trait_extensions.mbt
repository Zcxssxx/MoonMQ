///|
/// Keep the derived trait methods that form part of the public API explicit.
pub extend SessionState with Eq::{not_equal, equal}

///|
pub extend SessionState with @debug.Debug::{to_repr}

///|
pub extend SessionError with Eq::{not_equal, equal}

///|
pub extend SessionError with @debug.Debug::{to_repr}

///|
/// Keep the derived trait methods that form part of the public API explicit.
pub extend BasicProperties with Eq::{not_equal, equal}

///|
pub extend BasicProperties with @debug.Debug::{to_repr}

///|
pub extend ContentHeader with Eq::{not_equal, equal}

///|
pub extend ContentHeader with @debug.Debug::{to_repr}

///|
pub extend ContentMessage with Eq::{not_equal, equal}

///|
pub extend ContentMessage with @debug.Debug::{to_repr}

///|
pub extend FieldValue with Eq::{not_equal, equal}

///|
pub extend FieldValue with @debug.Debug::{to_repr}

///|
pub extend FieldTable with Eq::{not_equal, equal}

///|
pub extend FieldTable with @debug.Debug::{to_repr}

///|
pub extend FrameKind with Eq::{not_equal, equal}

///|
pub extend FrameKind with @debug.Debug::{to_repr}

///|
pub extend AmqpError with Eq::{not_equal, equal}

///|
pub extend AmqpError with @debug.Debug::{to_repr}

///|
pub extend Method with Eq::{not_equal, equal}

///|
pub extend Method with @debug.Debug::{to_repr}

///|
/// Keep the derived trait methods that form part of the public API explicit.
pub extend ExchangeKind with Eq::{not_equal, equal}

///|
pub extend ExchangeKind with @debug.Debug::{to_repr}

///|
pub extend Message with Eq::{not_equal, equal}

///|
pub extend Message with @debug.Debug::{to_repr}

///|
pub extend Delivery with Eq::{not_equal, equal}

///|
pub extend Delivery with @debug.Debug::{to_repr}

///|
pub extend ConsumerHandle with Eq::{not_equal, equal}

///|
pub extend ConsumerHandle with @debug.Debug::{to_repr}

///|
pub extend BrokerError with Eq::{not_equal, equal}

///|
pub extend BrokerError with @debug.Debug::{to_repr}

///|
pub extend QueueStats with Eq::{not_equal, equal}

///|
pub extend QueueStats with @debug.Debug::{to_repr}

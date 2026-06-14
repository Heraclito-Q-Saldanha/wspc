mod app;
mod callback;
mod error;
mod macros;
mod room;
mod socket;
mod types;

pub use app::*;
pub use callback::*;
pub use error::*;
pub use room::*;
pub use socket::*;
pub use types::*;
pub use wspc_derive::*;

#[cfg(feature = "state")]
mod typemap;
#[cfg(feature = "state")]
pub(crate) use typemap::*;

#[cfg(all(feature = "uuid_v4", feature = "uuid_v7"))]
compile_error!("Features `uuid_v4` and `uuid_v7` are mutually exclusive. Enable only one of them.");

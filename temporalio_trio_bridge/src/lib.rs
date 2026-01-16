/*!
 * Temporalio Trio Bridge
 *
 * A PyO3-based bridge that enables fully async communication between
 * Python (Trio) and Rust (Tokio).
 *
 * Architecture:
 * - Single Rust thread running Tokio runtime
 * - Queue-based request/response pattern
 * - Non-blocking from Python side
 * - Callbacks delivered via trio.from_thread
 *
 * Based on ASYNC_BRIDGE_DESIGN.md and poc_async_bridge.py
 */

use pyo3::prelude::*;

mod bridge;
mod request;

pub use bridge::TrioAsyncBridge;
pub use request::{Request, RequestId};

/// PyO3 module initialization
#[pymodule]
fn temporalio_trio_bridge(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TrioAsyncBridge>()?;
    Ok(())
}

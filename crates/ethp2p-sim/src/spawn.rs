//! Sim-aware implementation of the `Spawn` runtime trait.

use std::future::Future;
use std::pin::Pin;

use ethp2p_broadcast::runtime::{JoinHandle, Spawn, TokioSpawn};

/// Spawns onto the ambient tokio runtime — in the sim harness, the
/// runner's current-thread runtime, where spawned tasks only make
/// progress while the runner is inside `block_on`.
///
/// This delegates to `TokioSpawn` because `JoinHandle`'s inner field
/// is private to `ethp2p-broadcast`, so external `Spawn` impls cannot
/// construct one directly. The engine does not spawn tasks today; a
/// richer deterministic task scheduler is deferred until it does.
///
/// # Panics
///
/// Panics (inside tokio) if called outside a tokio runtime context.
#[derive(Debug, Clone, Copy, Default)]
pub struct SimSpawn;

impl Spawn for SimSpawn {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> JoinHandle {
        TokioSpawn.spawn(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawned_task_runs_on_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime");
        let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
        rt.block_on(async {
            SimSpawn.spawn(Box::pin(async move {
                tx.send(7).expect("receiver alive");
            }));
            assert_eq!(rx.await.expect("task ran"), 7);
        });
    }
}

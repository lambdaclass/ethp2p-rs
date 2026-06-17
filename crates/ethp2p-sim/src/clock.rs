//! Virtual-time implementation of the `Clock` runtime trait.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ethp2p_broadcast::runtime::Clock;

use crate::state::SimState;

/// Deterministic clock over the simulation's virtual time.
///
/// `now()` is the simulation start instant plus the virtual offset;
/// it changes only when the runner advances the simulation. `sleep`
/// futures resolve only when virtual time passes their wakeup point —
/// a 1-hour virtual sleep costs no wall-clock time.
#[derive(Debug, Clone)]
pub struct SimClock {
    shared: Arc<Mutex<SimState>>,
}

impl SimClock {
    pub(crate) fn new(shared: Arc<Mutex<SimState>>) -> Self {
        Self { shared }
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        self.shared.lock().expect("sim state mutex").wall_now()
    }

    fn sleep(&self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let mut state = self.shared.lock().expect("sim state mutex");
        let at = state.now() + d;
        Box::pin(state.schedule_wakeup(at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{FaultPlan, SimNetHub};
    use futures::FutureExt;

    #[test]
    fn sleep_resolves_on_virtual_advance_not_wall_clock() {
        let hub = SimNetHub::new(FaultPlan::new(0));
        let clock = hub.clock();
        let before = clock.now();

        let mut sleep = clock.sleep(Duration::from_hours(1));
        assert!(
            (&mut sleep).now_or_never().is_none(),
            "sleep must not resolve before virtual time advances"
        );

        let wall_start = Instant::now();
        hub.advance_to(Duration::from_hours(1));
        assert!((&mut sleep).now_or_never().is_some());
        assert!(
            wall_start.elapsed() < Duration::from_mins(1),
            "virtual advance must not consume wall-clock time"
        );
        assert!(clock.now() >= before + Duration::from_hours(1));
    }

    #[test]
    fn time_is_frozen_between_advances() {
        let hub = SimNetHub::new(FaultPlan::new(0));
        let clock = hub.clock();
        assert_eq!(clock.now(), clock.now());

        hub.advance_to(Duration::from_millis(250));
        let after = clock.now();
        assert_eq!(after, clock.now());
        assert_eq!(hub.virtual_now(), Duration::from_millis(250));
    }
}

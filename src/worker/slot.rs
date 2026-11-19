//! What occupies a worker: nothing, a runtime, or a runtime whose end could not be confirmed.
//!
//! A [`Vacant`] is made only here, at startup and by an ending that was confirmed, and starting a runtime takes it.
//! So no runtime starts beside one that may still be reading its sources.

use std::sync::Arc;
use std::time::Duration;

use super::child::{self, RuntimeHandle, RuntimeInbox};
use crate::leader::git::ContentId;

/// Nothing runs on this worker.
pub(super) struct Vacant(());

pub(super) enum Slot {
    Vacant(Vacant),
    Running {
        handle: RuntimeHandle,
        inbox: Arc<RuntimeInbox>,
        /// What the runtime was built from. A later commit whose group subtree and `shared/` are both
        /// unchanged runs the same configuration, so it is a commit to represent rather than one to restart for.
        content: ContentId,
    },
    /// A runtime whose exit this worker could not confirm. It may still be holding a source, so nothing takes
    /// its place; saying so and going on heartbeating is all that is left until an operator has seen to it.
    Blocked {
        handle: RuntimeHandle,
        inbox: Arc<RuntimeInbox>,
    },
}

/// What [`exited`] found of a runtime.
pub(super) enum Exited {
    Running(RuntimeHandle),
    Unconfirmed(RuntimeHandle),
    Confirmed {
        vacant: Vacant,
        commit: String,
        exit: String,
    },
}

impl Vacant {
    pub(super) fn at_startup() -> Vacant {
        Vacant(())
    }

    /// Starts a runtime here. An `Err` means no process started, so the vacancy comes back with it.
    pub(super) fn start(
        self,
        spec: child::StartSpec,
        inbox: &Arc<RuntimeInbox>,
        sockets: &crate::io::receiver::Sockets,
        listens: &[crate::plan::topology::Listen],
    ) -> Result<RuntimeHandle, (Vacant, std::io::Error)> {
        RuntimeHandle::start(spec, inbox, sockets, listens).map_err(|e| (self, e))
    }
}

impl Slot {
    pub(super) fn handle_mut(&mut self) -> Option<&mut RuntimeHandle> {
        match self {
            Slot::Vacant(_) => None,
            Slot::Running { handle, .. } | Slot::Blocked { handle, .. } => Some(handle),
        }
    }

    pub(super) fn inbox(&self) -> Option<&Arc<RuntimeInbox>> {
        match self {
            Slot::Vacant(_) => None,
            Slot::Running { inbox, .. } | Slot::Blocked { inbox, .. } => Some(inbox),
        }
    }
}

/// Ends `handle` and confirms it. An `Err` means the end could not be confirmed, so the runtime comes back.
pub(super) fn end(
    mut handle: RuntimeHandle,
    graceful: Duration,
    hard: Duration,
    inbox: &RuntimeInbox,
) -> Result<Vacant, (RuntimeHandle, std::io::Error)> {
    match handle.stop(graceful, hard, inbox) {
        Ok(()) => Ok(Vacant(())),
        Err(e) => Err((handle, e)),
    }
}

/// Whether `handle` has ended on its own, and whether everything it started has too.
pub(super) fn exited(mut handle: RuntimeHandle) -> Exited {
    match handle.exited() {
        child::Exit::Running => Exited::Running(handle),
        child::Exit::Unconfirmed => Exited::Unconfirmed(handle),
        child::Exit::Confirmed(exit) => Exited::Confirmed {
            vacant: Vacant(()),
            commit: handle.commit.clone(),
            exit,
        },
    }
}

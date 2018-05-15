use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{future, Async, Future, Poll, Stream};
use futures_mpsc_lossy::Receiver;
use tokio::executor::current_thread::TaskExecutor;

use super::event::Event;
use super::metrics;
use super::tap::Taps;
use connection;
use ctx;
use task::TaskError;

/// A `Control` which has been configured but not initialized.
#[derive(Debug)]
pub struct MakeControl {
    /// Receives events.
    rx: Receiver<Event>,

    process_ctx: Arc<ctx::Process>,

    metrics_retain_idle: Duration,
}

/// Handles the receipt of events.
///
/// `Control` exposes a `Stream` that summarizes events accumulated over the past
/// `flush_interval`.
///
/// As `Control` is polled, events are proceesed for the purposes of metrics export _as
/// well as_ for Tap, which supports subscribing to a stream of events that match
/// criteria.
///
/// # TODO
/// Limit the amount of memory that may be consumed for metrics aggregation.
#[derive(Debug)]
pub struct Control {
    /// Records telemetry events.
    metrics_record: metrics::Record,

    /// Serves scrapable metrics.
    metrics_service: metrics::Serve,

    /// Receives telemetry events.
    rx: Option<Receiver<Event>>,

    /// Holds the current state of tap observations, as configured by an external source.
    taps: Option<Arc<Mutex<Taps>>>,

}

// ===== impl MakeControl =====

impl MakeControl {
    /// Constructs a type that can instantiate a `Control`.
    ///
    /// # Arguments
    /// - `rx`: the `Receiver` side of the channel on which events are sent.
    /// - `process_ctx`: runtime process metadata.
    pub(super) fn new(
        rx: Receiver<Event>,
        process_ctx: &Arc<ctx::Process>,
        metrics_retain_idle: Duration,
    ) -> Self {
        Self {
            rx,
            process_ctx: Arc::clone(process_ctx),
            metrics_retain_idle,
        }
    }

    /// Bind a `Control` with the current task executor.
    ///
    /// # Arguments
    /// - `taps`: shares a `Taps` instance.
    ///
    /// # Returns
    /// - `Ok(())` if the timeout was successfully created.
    /// - `Err(io::Error)` if the timeout could not be created.
    pub fn make_control(self, taps: &Arc<Mutex<Taps>>) -> io::Result<Control> {
        let (metrics_record, metrics_service) =
            metrics::new(&self.process_ctx, self.metrics_retain_idle);

        Ok(Control {
            metrics_record,
            metrics_service,
            rx: Some(self.rx),
            taps: Some(taps.clone()),
        })
    }
}

// ===== impl Control =====

impl Control {
    fn recv(&mut self) -> Poll<Option<Event>, ()> {
        match self.rx.take() {
            None => Ok(Async::Ready(None)),
            Some(mut rx) => {
                match rx.poll() {
                    Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
                    ev => {
                        self.rx = Some(rx);
                        ev
                    }
                }
            }
        }
    }

    pub fn serve_metrics(&self, bound_port: connection::BoundPort)
        -> impl Future<Item = (), Error = io::Error>
    {
        use hyper;
        let service = self.metrics_service.clone();
        bound_port.listen_and_fold(
            hyper::server::conn::Http::new()
            move |hyper, (conn, _)| {
                let service = service.clone();
                let serve = hyper.serve_connection(conn, service)
                    .map(|_| {})
                    .map_err(|e| {
                        error!("error serving prometheus metrics: {:?}", e);
                    });
                let serve = ::logging::context_future("serve_metrics", serve);

                let r = TaskExecutor::current()
                    .spawn_local(Box::new(serve))
                    .map(move |()| hyper)
                    .map_err(TaskError::into_io);

                future::result(r)
            })
    }

}

impl Future for Control {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match try_ready!(self.recv()) {
                Some(ev) => {
                    if let Some(taps) = self.taps.as_mut() {
                        if let Ok(mut t) = taps.lock() {
                            t.inspect(&ev);
                        }
                    }

                    self.metrics_record.record_event(&ev);
                }
                None => {
                    debug!("events finished");
                    return Ok(Async::Ready(()));
                }
            };
        }
    }
}

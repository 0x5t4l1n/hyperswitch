#![cfg(feature = "deja")]
// Integration test: assertions use panic!/expect()/unwrap().
#![allow(clippy::panic, clippy::expect_used, clippy::unwrap_used)]
//! Characterization test for the correlation's post-response tail.
//!
//! A request's recording is closed when the ingress middleware's response future
//! resolves: `RecordingDecisionGuard` drops and the correlation's entry leaves
//! deja's recording-decision registry. The correlation itself survives that — it
//! rides the tracing span — so detached work that outlives the response still
//! resolves a correlation, finds no decision for it, and is dropped by the
//! opt-in capture gate with nothing written to say so.
//!
//! This test pins that boundary as it stands today. The tail below does
//! everything right by the tracing convention: it carries the request span, so
//! it IS attributed. It is still not recorded, because attribution and
//! authorization travel separately and only one of them survives teardown.
//!
//! # This test is a tripwire, and its failure is the point
//!
//! deja is changing the decision to travel with the span rather than in a
//! registry evicted at teardown (span-carried, memoised, with engagement and
//! capture kept as separate predicates). When that lands, this tail starts
//! recording and the `SkipNoDecision` assertion below inverts to `Capture`.
//!
//! **That inversion is the acceptance signal for the deja change, not a
//! regression here.** When it fires, flip the expectation rather than working
//! around it, and the vendor sites that spawn detached work with
//! `.in_current_span()` are then correct with no further change. Pre-registered
//! in `deja-handovers/CONVERGENCE.md`.
//!
//! Runs in its own integration-test binary so the process-global runtime hook
//! and tracing subscriber install exactly once.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use actix_web::{test, web, App, HttpResponse};
use router_env::request_id::{
    RequestIdentifier, RequestRecordingFacts, RequestRecordingSampler,
    RequestRecordingSamplerFuture,
};
use tracing::Instrument;
use tracing_subscriber::prelude::*;

/// Synchronous in-memory sink so the test can read back what the recorder wrote.
#[derive(Clone)]
struct VecSink(Arc<Mutex<Vec<deja::DejaRecord>>>);

impl deja::RecordSink<deja::DejaRecord> for VecSink {
    fn write_batch(&mut self, records: &[deja::DejaRecord]) -> std::io::Result<()> {
        let mut sink = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("sink lock poisoned"))?;
        sink.extend(records.iter().cloned());
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct RecordEverything;

impl RequestRecordingSampler for RecordEverything {
    fn should_record(&self, _facts: RequestRecordingFacts) -> RequestRecordingSamplerFuture<'_> {
        Box::pin(async { true })
    }
}

struct Tail {
    gate: Arc<AtomicBool>,
    verdict: Arc<Mutex<Option<deja::CaptureVerdict>>>,
}

/// Cross a boundary exactly as an instrumented call site does: consult the
/// per-boundary capture gate, and emit an event only if it says to. Returns the
/// verdict so a skip can be named rather than inferred from an empty sink.
fn cross_a_boundary() -> deja::CaptureVerdict {
    let hook = deja::installed_runtime_hook().expect("runtime hook installed");
    let verdict = deja::DejaHook::capture_verdict(hook.as_ref());
    if verdict.should_capture() {
        let event = deja::EventBuilder::start(
            hook.as_ref(),
            "tail_probe",
            "TailProbe",
            "post_response_tail",
            std::panic::Location::caller(),
            serde_json::json!({ "marker": "post_response_tail" }),
        );
        event.finish(hook.as_ref(), serde_json::json!({ "done": true }), false);
    }
    verdict
}

/// Park until the test releases the tail, so the boundary below is guaranteed to
/// be crossed after the ingress middleware has closed the recording.
async fn wait_for(gate: &AtomicBool) {
    for _ in 0..10_000 {
        if gate.load(Ordering::SeqCst) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("tail was never released");
}

async fn spawn_a_tail(tail: web::Data<Tail>) -> HttpResponse {
    // The shape every detached site in the router uses: the span, and so the
    // correlation, travels with the child. The recording decision does not.
    let gate = Arc::clone(&tail.gate);
    let verdict = Arc::clone(&tail.verdict);
    let _detached = tokio::spawn(
        async move {
            wait_for(&gate).await;
            *verdict.lock().unwrap() = Some(cross_a_boundary());
        }
        .in_current_span(),
    );

    HttpResponse::Ok().body(r#"{"ok":true}"#)
}

#[actix_web::test]
async fn a_detached_tail_keeps_its_correlation_and_loses_its_recording_decision() {
    tracing_subscriber::registry()
        .with(deja::DejaCorrelationLayer::new())
        .try_init()
        .expect("install correlation layer (own process)");

    let records = Arc::new(Mutex::new(Vec::new()));
    let hook = Arc::new(deja::RecordingHook::with_sink(
        VecSink(Arc::clone(&records)),
        "tail-capture-it".to_string(),
        deja::WriterConfig::default(),
    ));
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::Recording(hook)))
        .expect("install record hook (own process)");

    let verdict: Arc<Mutex<Option<deja::CaptureVerdict>>> = Arc::new(Mutex::new(None));
    let gate = Arc::new(AtomicBool::new(false));

    let sampler: Arc<dyn RequestRecordingSampler> = Arc::new(RecordEverything);
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(Tail {
                gate: Arc::clone(&gate),
                verdict: Arc::clone(&verdict),
            }))
            .wrap(RequestIdentifier::new("x-request-id").with_recording_sampler(sampler))
            .route("/payments", web::post().to(spawn_a_tail)),
    )
    .await;

    let request = test::TestRequest::post().uri("/payments").to_request();
    let response = test::call_service(&app, request).await;
    assert!(response.status().is_success(), "handler should return 200");

    // Drive the body to EOF: the http_incoming event finalizes here, and the
    // ingress middleware has already dropped the recording decision.
    let _body = test::read_body(response).await;

    gate.store(true, Ordering::SeqCst);
    for _ in 0..10_000 {
        if verdict.lock().unwrap().is_some() {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Synchronous drain barrier: flush blocks until the async writer has handed
    // every queued record to the sink.
    deja::flush_global_runtime_hook().expect("flush recording hook");

    let verdict = *verdict.lock().expect("verdict lock");
    assert_eq!(
        verdict,
        Some(deja::CaptureVerdict::SkipNoDecision),
        "a detached tail that carries the request span still loses the recording \
         decision once the ingress guard has dropped. If this now reads \
         `Capture`, deja's span-carried decision has landed — flip the \
         expectation, do not work around it (see the module docs)"
    );

    let recorded = records.lock().expect("sink lock");
    let probes = recorded
        .iter()
        .filter(|record| {
            matches!(record, deja::DejaRecord::BoundaryEvent(event) if event.boundary == "tail_probe")
        })
        .count();
    assert_eq!(
        probes, 0,
        "the tail's boundary must be absent from the tape while the decision is \
         gone; that silent absence is the defect the deja change removes"
    );
}

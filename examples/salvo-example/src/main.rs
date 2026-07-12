//! # salvo-example
//!
//! A complete example that wires **apalis-board** into a [Salvo](https://salvo.rs/)
//! server.  The setup mirrors the existing `axum-email-service` example so you
//! can compare the two side-by-side.
//!
//! ## Running
//!
//! ```shell
//! cargo run --package salvo-example
//! # Open: http://localhost:5800
//! ```
//!
//! ## Testing the API with curl
//!
//! ```shell
//! # List all workers
//! curl http://localhost:5800/api/v1/workers
//!
//!
//!
//! # Stream live task logs (Ctrl-C to stop)
//! curl -N http://localhost:5800/api/v1/events
//! ```

use apalis::prelude::*;
use apalis_board::salvo::framework::salvo::ui::ServeApp;
use apalis_board::salvo::{
    framework::{ApiBuilder, RegisterRoute},
    sse::{TracingBroadcaster, TracingSubscriber},
};
use apalis_sqlite::{SqlitePool, SqliteStorage};
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

// ─── Job type ─────────────────────────────────────────────────────────────────

/// A simple "send email" background job.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Email {
    to: String,
    subject: String,
}

/// Handler — would actually send an email here.
async fn send_email(email: Email) {
    info!(
        to = %email.to,
        subject = %email.subject,
        "📧 processing email job"
    );
    // Simulate I/O work.
    tokio::time::sleep(Duration::from_millis(150)).await;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. TracingBroadcaster ─────────────────────────────────────────────────
    //
    // This broadcasts every tracing span/event emitted by apalis workers to all
    // SSE subscribers connected at GET /api/v1/events.
    //
    // It MUST be set up before the tracing subscriber is initialised so that
    // the layer is registered with the global registry.
    let broadcaster = TracingBroadcaster::create();
    let tracing_subscriber_inner = TracingSubscriber::new(&broadcaster);

    tracing_subscriber::registry()
        // Stream task logs to the board's live-log UI page.
        .with(
            tracing_subscriber_inner
                .layer()
                .with_filter(EnvFilter::builder().parse("debug").unwrap()),
        )
        // Also print to stdout so you can see what's happening in your terminal.
        .with(tracing_subscriber::fmt::layer())
        .init();

    // ── 2. SQLite storage ─────────────────────────────────────────────────────
    //
    // Creates (or opens) `example.db` in the current directory.
    // `SqliteStorage::setup` runs the apalis migrations on first start.
    //
    // Note: `apalis_sqlite::SqlitePool` is this crate's own re-export — it's no
    // longer `sqlx::SqlitePool` directly, and `SqliteStorage::new` now takes the
    // pool by reference rather than an owned value.
    let pool = SqlitePool::connect("sqlite:example.db?mode=rwc").await?;
    SqliteStorage::setup(&pool).await?;

    let email_storage = SqliteStorage::new(&pool);

    // ── 3. Background job producer ────────────────────────────────────────────
    //
    // Pushes a new Email job every 3 seconds so there's always something to see
    // in the board UI.
    let mut producer = email_storage.clone();
    tokio::spawn(async move {
        let mut n = 0u32;
        loop {
            n += 1;
            let job = Email {
                to: format!("user{}@example.com", n),
                subject: format!("Newsletter #{}", n),
            };
            if let Err(e) = producer.push(job).await {
                tracing::error!("push failed: {e}");
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    // ── 5. apalis-board API router (Salvo) ────────────────────────────────────
    //
    // `.register(email_storage)` exposes the Email queue under the URL segment
    // derived from the type name, e.g. "Email" → /api/v1/Email/jobs.
    //
    // `.with_broadcaster(broadcaster)` enables the SSE live-log endpoint.
    let api_router = ApiBuilder::new(Router::new())
        .with_broadcaster(broadcaster.clone())
        .register(email_storage.clone())
        .build();

    // ── 6. Full Salvo router ──────────────────────────────────────────────────
    //
    // Route layout:
    //   /api/v1/**   → board REST API
    //   /**          → embedded board UI (SPA)
    let app = Router::new()
        .push(Router::with_path("api/v1").push(api_router))
        .push(ServeApp::router()); // catch-all SPA fallback

    // ── 7. Run both the Salvo server and the apalis monitor concurrently ───────
    let monitor = Monitor::new().register(move |index| {
        WorkerBuilder::new(format!("email-worker-{index}"))
            .backend(email_storage.clone())
            .enable_tracing()
            .build(send_email)
    });

    info!("🚀 apalis-board (Salvo) → http://0.0.0.0:5800");

    tokio::select! {
        result = async {
            let acceptor = TcpListener::new("0.0.0.0:5800").bind().await;
            Server::new(acceptor).serve(app).await;
            Ok::<_, anyhow::Error>(())
        } => result?,

        result = monitor.run() => result?,
    }

    Ok(())
}

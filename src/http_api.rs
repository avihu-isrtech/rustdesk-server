use crate::database::{self, Database, Peer};
use async_stream::stream;
use axum::{
    extract::{Extension, Path, Query},
    http::{header::AUTHORIZATION, Method, Request, StatusCode},
    middleware::{from_fn, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Response,
    },
    routing::{get, patch},
    Json, Router,
};
use futures_core::stream::Stream;
use hbb_common::{log, tokio, ResultType};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{convert::Infallible, net::SocketAddr, process, sync::Arc, thread, time::Duration};
use tokio::sync::broadcast::{self, error::RecvError};
use tower_http::cors::CorsLayer;

const HTTP_PORT: u16 = 37_000;
const EVENT_BUFFER: usize = 128;
const KEEP_ALIVE_SECS: u64 = 15;
static PEER_EVENT_BUS: OnceCell<broadcast::Sender<PeerEvent>> = OnceCell::new();

#[derive(Clone)]
struct ApiState {
    db: Database,
    peer_events: broadcast::Sender<PeerEvent>,
    auth_token: Arc<String>,
}

#[derive(Serialize)]
struct PeerResponse {
    guid: String,
    id: Option<String>,
    uuid: Option<String>,
    public_key: Option<String>,
    created_at: Option<i64>,
    last_heartbeat: Option<i64>,
    user: Option<String>,
    status: Option<i64>,
    info: Option<String>,
    note: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PeerEvent {
    kind: &'static str,
    peer_id: String,
    peer_guid: String,
    peer: Option<Peer>
}

impl PeerEvent {
    fn registered(peer: &Peer) -> Self {
        Self {
            kind: "peer_registered",
            peer_id: peer.id.to_owned(),
            peer_guid: base64::encode(peer.guid.to_owned()),
            peer: Option::from(peer.to_owned()),
        }
    }

    fn possible_disconnection(id: &str, guid: &str) -> Self {
        Self {
            kind: "possible_disconnection",
            peer_id: id.to_owned(),
            peer_guid: guid.to_owned(),
            peer: None,
        }
    }

    fn heartbeat(id: &str, guid: &str) -> Self {
        Self {
            kind: "device_heartbeat",
            peer_id: id.to_owned(),
            peer_guid: guid.to_owned(),
            peer: None,
        }
    }
}

impl From<Peer> for PeerResponse {
    fn from(peer: Peer) -> Self {
        Self {
            guid: base64::encode(peer.guid),
            id: Option::from(peer.id),
            uuid: Option::from(base64::encode(peer.uuid)),
            public_key: Option::from(base64::encode(peer.pk)),
            created_at: Option::from(peer.created_at),
            last_heartbeat: Option::from(peer.last_heartbeat),
            user: peer.user.map(base64::encode),
            status: peer.status,
            info: Option::from(peer.info),
            note: peer.note,
        }
    }
}

pub fn spawn_http_server() {
    if let Err(err) = thread::Builder::new().name("http-api".into()).spawn(|| {
        if let Err(err) = run_http_server_blocking() {
            log::error!("HTTP server stopped: {}", err);
        }
    }) {
        log::error!("Failed to spawn HTTP server: {}", err);
    }
}

fn run_http_server_blocking() -> ResultType<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("http-api-worker")
        .build()?;

    runtime.block_on(async move {
        let db_url = database::resolve_db_url();
        let db = Database::new(&db_url).await?;
        let events = peer_event_sender();
        let auth_token = load_auth_token();
        let state = ApiState {
            db,
            peer_events: events,
            auth_token: auth_token.clone(),
        };
        run_http_server(state, auth_token).await
    })
}

async fn run_http_server(state: ApiState, auth_token: Arc<String>) -> ResultType<()> {
    let shared_state = Arc::new(state);
    let header_token = auth_token.clone();

    let protected_routes = Router::new()
        .route("/api/v1/peers", get(list_peers))
        .route("/api/v1/peers/:guid", get(get_peer).delete(delete_peer))
        .route("/api/v1/peers/:guid/note", patch(update_note))
        .route("/api/v1/peers/:guid/info", patch(update_info))
        .route("/api/v1/peers/:guid/user", patch(update_user))
        .layer(from_fn({
            let token = header_token.clone();
            move |req, next| {
                let token = token.clone();
                async move { ensure_authorized(req, next, token).await }
            }
        }));

    let sse_route = Router::new().route("/api/v1/events/peers", get(peer_events_stream));

    let app = protected_routes
        .merge(sse_route)
        .layer(Extension(shared_state))
        .layer(CorsLayer::permissive());
    let addr = SocketAddr::from(([0, 0, 0, 0], HTTP_PORT));
    log::info!("Listening on http :{}", addr.port());
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn list_peers(
    Extension(state): Extension<Arc<ApiState>>,
) -> Result<Json<Vec<PeerResponse>>, StatusCode> {
    match state.db.get_peers().await {
        Ok(peers) => Ok(Json(
            peers
                .into_iter()
                .map(PeerResponse::from)
                .collect::<Vec<_>>(),
        )),
        Err(err) => {
            log::error!("Failed to fetch peers: {}", err);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_peer(
    Path(guid): Path<String>,
    Extension(state): Extension<Arc<ApiState>>,
) -> Result<Json<PeerResponse>, StatusCode> {
    let guid_bytes = decode_guid(&guid)?;
    match state.db.get_peer_by_guid(&guid_bytes).await {
        Ok(Some(peer)) => Ok(Json(PeerResponse::from(peer))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(err) => {
            log::error!("Failed to fetch peer: {}", err);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct NotePayload {
    note: String,
}

#[derive(Deserialize)]
struct InfoPayload {
    info: String,
}

#[derive(Deserialize)]
struct UserPayload {
    user: Option<String>,
}

#[derive(Deserialize)]
struct SseAuthQuery {
    token: String,
}

async fn update_note(
    Path(guid): Path<String>,
    Extension(state): Extension<Arc<ApiState>>,
    Json(payload): Json<NotePayload>,
) -> Result<StatusCode, StatusCode> {
    let guid_bytes = decode_guid(&guid)?;
    state
        .db
        .update_note(&guid_bytes, &payload.note)
        .await
        .map_err(|err| {
            log::error!("Failed to update note: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_info(
    Path(guid): Path<String>,
    Extension(state): Extension<Arc<ApiState>>,
    Json(payload): Json<InfoPayload>,
) -> Result<StatusCode, StatusCode> {
    let guid_bytes = decode_guid(&guid)?;
    state
        .db
        .update_info(&guid_bytes, &payload.info)
        .await
        .map_err(|err| {
            log::error!("Failed to update info: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_user(
    Path(guid): Path<String>,
    Extension(state): Extension<Arc<ApiState>>,
    Json(payload): Json<UserPayload>,
) -> Result<StatusCode, StatusCode> {
    let guid_bytes = decode_guid(&guid)?;
    let user_bytes = payload.user.map(|u| u.into_bytes());
    state
        .db
        .update_user(&guid_bytes, user_bytes.as_deref())
        .await
        .map_err(|err| {
            log::error!("Failed to update user: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_peer(
    Path(guid): Path<String>,
    Extension(state): Extension<Arc<ApiState>>,
) -> Result<StatusCode, StatusCode> {
    let guid_bytes = decode_guid(&guid)?;
    state.db.delete_peer(&guid_bytes).await.map_err(|err| {
        log::error!("Failed to delete peer: {}", err);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn peer_events_stream(
    Extension(state): Extension<Arc<ApiState>>,
    Query(params): Query<SseAuthQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    if params.token != *state.auth_token {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let mut receiver = state.peer_events.subscribe();
    let event_stream = stream! {
        loop {
            match receiver.recv().await {
                Ok(event) => match serde_json::to_string(&event) {
                    Ok(payload) => yield Ok(Event::default().event(event.kind).data(payload)),
                    Err(err) => {
                        log::error!("Failed to serialize peer event: {}", err);
                    }
                },
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!("Peer event stream lagged, skipped {skipped} events");
                }
                Err(RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(KEEP_ALIVE_SECS))
            .text("keep-alive"),
    ))
}

pub fn notify_peer_registered(peer: &Peer) {
    emit_event(|| PeerEvent::registered(peer), &peer.id);
}

pub fn notify_peer_heartbeat(id: &str, guid: &str) {
    emit_event(|| PeerEvent::heartbeat(id, guid), id);
}

pub fn notify_peer_possible_disconnection(id: &str, guid: &str) {
    emit_event(|| PeerEvent::possible_disconnection(id, guid), id);
}

fn peer_event_sender() -> broadcast::Sender<PeerEvent> {
    PEER_EVENT_BUS
        .get_or_init(|| {
            let (tx, _rx) = broadcast::channel(EVENT_BUFFER);
            tx
        })
        .clone()
}

fn emit_event<F>(builder: F, id: &str)
where
    F: FnOnce() -> PeerEvent,
{
    if should_skip_peer(id) {
        return;
    }
    let sender = peer_event_sender();
    if let Err(err) = sender.send(builder()) {
        log::trace!("Failed to broadcast peer event: {}", err);
    }
}

fn should_skip_peer(id: &str) -> bool {
    id == "(:test_hbbs:)"
}

fn load_auth_token() -> Arc<String> {
    let token = std::env::var("HTTP_API_TOKEN").unwrap_or_else(|_| {
        log::error!("HTTP_API_TOKEN environment variable must be set");
        process::exit(1);
    });
    if token.is_empty() {
        log::error!("HTTP_API_TOKEN cannot be empty");
        process::exit(1);
    }
    Arc::new(token)
}

async fn ensure_authorized<B>(
    req: Request<B>,
    next: Next<B>,
    expected: Arc<String>,
) -> Result<Response, StatusCode> {
    if req.method() == Method::OPTIONS {
        return Ok(next.run(req).await);
    }
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if header != Some(expected.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

fn decode_guid(guid: &str) -> Result<Vec<u8>, StatusCode> {
    base64::decode(guid).map_err(|_| StatusCode::BAD_REQUEST)
}

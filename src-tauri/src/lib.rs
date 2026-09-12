use dns_lookup::lookup_addr;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::collections::HashSet;
use std::net::IpAddr;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tauri::{AppHandle, Emitter};

fn resolve_hostname(ip: IpAddr) -> Result<String, String> {
    lookup_addr(&ip).map_err(|e| e.to_string())
}
use serde::Deserialize;

#[derive(Deserialize, serde::Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PeerInfo {
    #[serde(rename = "HostName")]
    pub host_name: String,
    #[serde(rename = "DNSName")]
    pub dns_name: String,
    #[serde(rename = "TailscaleIPs")]
    pub tailscale_ips: Vec<String>,
    pub online: Option<bool>,
}

const TAILSCALE_PATHS: &[&str] = &[
    "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
    "/Applications/Tailscale.app/Contents/PlugIns/IPNExtension.appex/Contents/MacOS/Tailscale",
    "/usr/local/bin/tailscale",
    "tailscale",
];

#[derive(Default)]
struct MqttClients {
    clients: Mutex<Vec<Arc<Mqtt>>>,
    next_handle: AtomicU64,
}

struct Mqtt {
    handle: u64,
    client_id: String,
    client: AsyncClient,
    topic_list: Mutex<HashSet<String>>,
    ip: String,
    hostname: Option<String>,
    port: u16,
    task: tauri::async_runtime::JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

#[derive(Clone, serde::Serialize)]
struct MqttMessage {
    connection: String,
    topic: String,
    payload: String,
    retain: bool,
    qos: u8,
}

fn with_client(state: &MqttClients, handle: u64) -> Result<AsyncClient, String> {
    let guard = state.clients.lock().map_err(|e| e.to_string())?;
    guard
        .iter()
        .find(|c| c.handle == handle)
        .map(|c| c.client.clone())
        .ok_or_else(|| format!("no connection {handle}"))
    // guard drops here, before you .await on the returned client
}

fn get_conn(state: &MqttClients, handle: u64) -> Result<Arc<Mqtt>, String> {
    let guard = state.clients.lock().map_err(|e| e.to_string())?;
    guard
        .iter()
        .find(|c| c.handle == handle)
        .cloned()
        .ok_or_else(|| format!("no connection {handle}"))
}

#[tauri::command]
async fn mqtt_publish(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
    topic: String,
    payload: String,
) -> Result<(), String> {
    let client = with_client(&state, handle)?;
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.into_bytes())
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn mqtt_connect(
    app: AppHandle,
    state: tauri::State<'_, MqttClients>,
    ip: String,
    port: u16,
) -> Result<u64, String> {
    let handle = state.next_handle.fetch_add(1, Ordering::Relaxed);
    let client_id = format!("ye-{}-{}", std::process::id(), handle);

    let hostname = resolve_hostname(ip.parse::<IpAddr>().map_err(|e| e.to_string())?).ok();
    let mut opts = MqttOptions::new(&client_id, &ip, port);
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let connected = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

    let task = {
        let app = app.clone();
        let connected = Arc::clone(&connected);
        let id = client_id.clone();
        let mut ready = Some(ready_tx);
        let client_id = client_id.clone();

        tauri::async_runtime::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                        if ack.code != rumqttc::ConnectReturnCode::Success {
                            if let Some(tx) = ready.take() {
                                let _ = tx.send(Err(format!("broker refused: {:?}", ack.code)));
                            }
                            continue;
                        }
                        connected.store(true, Ordering::Release);
                        if let Some(tx) = ready.take() {
                            let _ = tx.send(Ok(()));
                        }
                        let _ = app.emit("mqtt-state", (&id, true));
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let _ = app.emit(
                            "mqtt-message",
                            MqttMessage {
                                connection: client_id.clone(),
                                topic: p.topic,
                                payload: String::from_utf8_lossy(&p.payload).to_string(),
                                retain: p.retain,
                                qos: p.qos as u8,
                            },
                        );
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        connected.store(false, Ordering::Release);
                        let _ = app.emit("mqtt-state", (&id, false));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        connected.store(false, Ordering::Release);
                        if let Some(tx) = ready.take() {
                            let _ = tx.send(Err(e.to_string()));
                        }
                        let _ = app.emit("mqtt-error", (&id, e.to_string()));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    };

    match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            task.abort();
            return Err(e);
        }
        Ok(Err(_)) => {
            task.abort();
            return Err("connect task died".into());
        }
        Err(_) => {
            task.abort();
            return Err(format!("timed out connecting to {ip}:{port}"));
        }
    }

    state
        .clients
        .lock()
        .map_err(|e| e.to_string())?
        .push(Arc::new(Mqtt {
            handle,
            client_id,
            ip,
            hostname,
            port,
            client,
            topic_list: Mutex::new(HashSet::new()),
            connected,
            task,
        }));

    Ok(handle)
}

#[tauri::command]
async fn mqtt_disconnect(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
) -> Result<String, String> {
    let client = with_client(&state, handle)?;
    client.disconnect().await.map_err(|e| e.to_string())?;

    // rmeove client from the list
    state
        .clients
        .lock()
        .map_err(|e| e.to_string())?
        .retain(|c| c.handle != handle);

    Ok("Disconnected".into())
}

#[tauri::command]
async fn mqtt_subscribe(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
    topic: String,
) -> Result<(), String> {
    let conn = get_conn(&state, handle)?;

    conn.client
        .subscribe(topic.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| e.to_string())?;

    conn.topic_list
        .lock()
        .map_err(|e| e.to_string())?
        .insert(topic);

    Ok(())
}

#[tauri::command]
async fn mqtt_unsubscribe(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
    topic: String,
) -> Result<(), String> {
    let conn = get_conn(&state, handle)?;

    conn.client
        .unsubscribe(topic.clone())
        .await
        .map_err(|e| e.to_string())?;

    conn.topic_list
        .lock()
        .map_err(|e| e.to_string())?
        .remove(&topic);

    Ok(())
}

#[tauri::command]
async fn get_subscribe_list(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
) -> Result<Vec<String>, String> {
    let conn = get_conn(&state, handle)?;
    let topics = conn.topic_list.lock().map_err(|e| e.to_string())?;
    Ok(topics.iter().cloned().collect())
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MqttConnectionInfo {
    ip: String,
    hostname: Option<String>,
    port: u16,
    connected: Arc<AtomicBool>,
    handle: u64,
}

#[tauri::command]
async fn is_connected(
    state: tauri::State<'_, MqttClients>,
    handle: u64,
) -> Result<MqttConnectionInfo, String> {
    let conn = get_conn(&state, handle)?;
    let ip = conn.ip.clone();
    let hostname = conn.hostname.clone();
    let port = conn.port;
    let connected = conn.connected.clone();
    let handle = conn.handle;
    Ok(MqttConnectionInfo {
        ip,
        hostname,
        port,
        connected,
        handle,
    })
}

#[tauri::command]
async fn get_all_clients(
    state: tauri::State<'_, MqttClients>,
) -> Result<Vec<MqttConnectionInfo>, String> {
    let clients = state.clients.lock().map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    for conn in clients.iter() {
        result.push(MqttConnectionInfo {
            ip: conn.ip.clone(),
            hostname: conn.hostname.clone(),
            port: conn.port,
            connected: conn.connected.clone(),
            handle: conn.handle,
        });
    }
    Ok(result)
}

#[tauri::command]
async fn get_peer_info() -> Result<Vec<PeerInfo>, String> {
    // 1. Loop through paths using .await
    for path in TAILSCALE_PATHS {
        if let Ok(output) = Command::new(path).arg("status").arg("--json").output() {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);

                // Parse directly into a dynamic JSON value
                let json: serde_json::Value =
                    serde_json::from_str(&stdout).map_err(|e| e.to_string())?;

                // Extract only the "Peer" block, ignoring everything else
                if let Some(peer_map) = json.get("Peer") {
                    let peers: Vec<PeerInfo> = serde_json::from_value(peer_map.clone())
                        .map(|map: std::collections::HashMap<String, PeerInfo>| {
                            map.into_values().collect()
                        })
                        .map_err(|e| e.to_string())?;
                    return Ok(peers);
                }
            }
        }
    }
    Err("No valid Tailscale path found or failed to get peer info".into())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_store::Builder::new().build())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_liquid_glass::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(MqttClients::default())
        .invoke_handler(tauri::generate_handler![
            mqtt_publish,
            mqtt_connect,
            mqtt_disconnect,
            mqtt_subscribe,
            get_subscribe_list,
            mqtt_unsubscribe,
            is_connected,
            get_all_clients,
            get_peer_info
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

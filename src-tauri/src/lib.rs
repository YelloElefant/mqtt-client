use dns_lookup::lookup_addr;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};

fn resolve_hostname(ip: IpAddr) -> Result<String, String> {
    lookup_addr(&ip).map_err(|e| e.to_string())
}

#[derive(Default)]
struct Mqtt {
    client: Mutex<Option<AsyncClient>>,
    topic_list: Mutex<Vec<String>>,
    ip: Mutex<Option<String>>,
    hostname: Mutex<Option<String>>,
    port: Mutex<Option<u16>>,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MqttMessage {
    topic: String,
    payload: String,
    retain: bool,
    qos: u8,
}

#[tauri::command]
async fn mqtt_publish(
    state: tauri::State<'_, Mqtt>,
    topic: String,
    payload: String,
) -> Result<(), String> {
    let client = {
        let guard = state.client.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    let client = client.ok_or("Not connected".to_string())?;
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.into_bytes())
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn mqtt_connect(
    app: AppHandle,
    state: tauri::State<'_, Mqtt>,
    host: String,
    port: u16,
) -> Result<String, String> {
    let mut mqttoptions = MqttOptions::new("tauri-client", host.as_str(), port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let host_c = host.clone();

    {
        let mut guard = state.client.lock().map_err(|e| e.to_string())?;
        *guard = Some(client);

        let mut ip_guard = state.ip.lock().map_err(|e| e.to_string())?;
        *ip_guard = Some(host);

        let mut port_guard = state.port.lock().map_err(|e| e.to_string())?;
        *port_guard = Some(port);

        let mut hostname_guard = state.hostname.lock().map_err(|e| e.to_string())?;
        if let Ok(ip) = host_c.parse::<IpAddr>() {
            if let Ok(hostname) = resolve_hostname(ip) {
                *hostname_guard = Some(hostname);
            }
        }
    }

    tauri::async_runtime::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let _ = app.emit(
                        "mqtt-message",
                        MqttMessage {
                            topic: publish.topic,
                            payload: String::from_utf8_lossy(&publish.payload).to_string(),
                            retain: publish.retain,
                            qos: publish.qos as u8,
                        },
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = app.emit("mqtt-error", e.to_string());
                    break;
                }
            }
        }
    });

    Ok("Connected".into())
}

#[tauri::command]
async fn mqtt_subscribe(state: tauri::State<'_, Mqtt>, topic: String) -> Result<String, String> {
    let topic_c = topic.clone();
    let client = {
        let guard = state.client.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    let client = client.ok_or("Not connected".to_string())?;
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .map_err(|e| e.to_string())?;

    {
        let mut guard = state.topic_list.lock().map_err(|e| e.to_string())?;
        guard.push(topic_c.clone());
    }

    Ok(format!("Subscribed {}", topic_c).into())
}

#[tauri::command]
async fn get_subscribe_list(state: tauri::State<'_, Mqtt>) -> Result<Vec<String>, String> {
    let client = {
        let guard = state.client.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    let client = client.ok_or("Not connected".to_string())?;

    // Assuming you have a way to get the list of subscribed topics from the client
    // This is a placeholder implementation
    let subscribe_list = {
        let guard = state.topic_list.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };

    Ok(subscribe_list)
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MqttConnectionInfo {
    ip: Option<String>,
    hostname: Option<String>,
    port: Option<u16>,
    connected: bool,
}

#[tauri::command]
async fn is_connected(state: tauri::State<'_, Mqtt>) -> Result<MqttConnectionInfo, String> {
    let client = {
        let guard = state.client.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    // get client ip and hostname
    let ip = {
        let guard = state.ip.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    let hostname = {
        let guard = state.hostname.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    let port = {
        let guard = state.port.lock().map_err(|e| e.to_string())?;
        guard.clone()
    };
    Ok(MqttConnectionInfo {
        ip,
        hostname,
        port,
        connected: client.is_some(),
    })
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_liquid_glass::init())
        .manage(Mqtt::default())
        .invoke_handler(tauri::generate_handler![
            mqtt_publish,
            mqtt_connect,
            mqtt_subscribe,
            get_subscribe_list,
            is_connected
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

// ── Tauri bridge, with a mock fallback for browser preview ──
const T = window.__TAURI__;
const invoke = T ? T.core.invoke : async (cmd, args) => mockInvoke(cmd, args);
const listen = T ? T.event.listen : (ev, cb) => mockListen(ev, cb);
const load = T ? T.store.load : async (key) => mockLoad(key);
const ask = T.dialog.message;
import { promptEditConnection } from './dialog.js';

let store;

async function loadStore() {
  store = await load('clients.json', { autoSave: true });
}

async function tail() {
  const peers = await invoke('get_peer_info');
  // remove local machine from the list of peers
  peers.findIndex(peer => peer.HostName === 'localhost') !== -1 && peers.splice(peers.findIndex(peer => peer.HostName === 'localhost'), 1);
  return peers;
}

const MqttClients = new Map(); // handle -> client info
let currentClient = null;

const closeSvg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="12" height="12">
  <path d="M18 6L6 18M6 6l12 12" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
</svg> `;


// ── Liquid Glass Effect (Tauri Mac OS vibrancy) ─────────────────────
async function initLiquidGlass() {
  if (T?.core?.invoke) {
    try {
      const supported = await invoke("plugin:liquid-glass|is_glass_supported");
      if (supported && T.window) {
        const appWindow = T.window.getCurrentWindow();
        await invoke("plugin:liquid-glass|set_liquid_glass_effect", {
          window: appWindow.label,
          config: {
            cornerRadius: 24,
          },
        });
      }
    } catch (e) {
      console.warn("Liquid glass error:", e);
    }
  }
}

// ── State ─────────────────────────────────────────────────────────
const MAX = 500;                 // ring buffer; a firehose will eat RAM otherwise
let messages = [];
let seq = 0;
let connected = false;
let qos = 0;
let host = { name: 'localhost', port: 1883 };

function parseHostPort(str) {
  let containsColon = str.includes(':');
  if (containsColon) {
    const [name, portStr] = str.split(':');
    const port = parseInt(portStr, 10);
    return { name, port: isNaN(port) ? 1883 : port };
  } else {
    return { name: str, port: 1883 };
  }
}

// ── Topic → colour. FNV-1a, take the low bits as a hue. ──────────
const hueCache = new Map();
function hue(topic) {
  if (hueCache.has(topic)) return hueCache.get(topic);
  let h = 2166136261;
  for (let i = 0; i < topic.length; i++) {
    h ^= topic.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  const deg = Math.abs(h) % 360;
  hueCache.set(topic, deg);
  return deg;
}
const topicColor = t => `hsl(${hue(t)} var(--topic-s) var(--topic-l))`;

// ── JSON helpers ──────────────────────────────────────────────────
function tryParse(s) { try { return JSON.parse(s); } catch { return null; } }

function highlight(text) {
  const esc = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return esc.replace(
    /("(\\.|[^"\\])*"\s*:)|("(\\.|[^"\\])*")|(-?\d+\.?\d*([eE][+-]?\d+)?)|\b(true|false|null)\b/g,
    m => {
      if (/:$/.test(m)) return `< span class="k" > ${m}</span > `;
      if (/^"/.test(m)) return `< span class="s" > ${m}</span > `;
      if (/^(true|false|null)$/.test(m)) return `< span class="b" > ${m}</span > `;
      return `< span class="n" > ${m}</span > `;
    }
  );
}

function peek(payload) {
  const o = tryParse(payload);
  if (o === null) return payload.slice(0, 200);
  return JSON.stringify(o).slice(0, 200);
}

function stamp(ts) {
  const d = new Date(ts);
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)} `;
}

const escapeHtml = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// ── DOM References ────────────────────────────────────────────────
let stream, filterEl, jumpBtn;
const openRows = new Set();

function visible() {
  const q = filterEl.value.trim().toLowerCase();
  if (!q) return messages;
  return messages.filter(m =>
    m.topic.toLowerCase().includes(q) || m.payload.toLowerCase().includes(q));
}

function renderStream() {
  if (!stream) return;
  const rows = visible();
  document.getElementById('count').textContent =
    `${rows.length} message${rows.length === 1 ? '' : 's'} `;

  if (!rows.length) {
    stream.innerHTML = connected
      ? `<div class="stream-empty">Subscribed topics will appear here as messages arrive.</div>`
      : '<div class="stream-empty">Connect to a broker to start watching traffic.</div>';
    return;
  }

  stream.innerHTML = rows.map(m => {
    const c = topicColor(m.topic);
    const open = openRows.has(m.id);
    const pretty = (() => {
      const o = tryParse(m.payload);
      return o === null ? m.payload : JSON.stringify(o, null, 2);
    })();
    return `
  <div class="msg ${open ? 'open' : ''} ${m.out ? 'out' : ''}" data - id="${m.id}" style = "color:${c}" >
        <span class="m-time">${stamp(m.ts)}</span>
        <span class="m-line">
          <span class="m-topic">${escapeHtml(m.topic)}</span>
          <span class="m-peek">${escapeHtml(peek(m.payload))}</span>
        </span>
        <span class="flags">
          ${m.retain ? '<span class="flag r">R</span>' : ''}
          <span class="flag q">Q${m.qos}</span>
        </span>
        ${open ? `<div class="m-body">${highlight(pretty)}</div>` : ''}
      </div> `;
  }).join('');
}

async function deleteTopic(topic) {
  const topicList = document.getElementById('topicList');
  const topicRow = topicList.querySelector(`[data-unsub="${escapeHtml(topic)}"]`)?.closest('.topic-row');
  if (topicRow) topicList.removeChild(topicRow);

  const subTopic = MqttClients.get(currentClient).topics.find(t => t.topic === topic);
  for (const sub of subTopic.subTopics) {
    deleteTopic(sub);
  }

  try { await invoke('mqtt_unsubscribe', { handle: currentClient, topic: topic }); } catch (e) { console.error(e) }

}

function addTopic(topic, isSubTopic = false, headTopic = null) {
  const topicList = document.getElementById('topicList');
  if (!topicList) return;
  const row = document.createElement('div');
  row.className = 'topic-row';
  row.style.color = topicColor(topic);

  const swatch = document.createElement('span');
  swatch.className = 'topic-swatch';
  row.appendChild(swatch);

  const name = document.createElement('span');
  name.className = 'topic-name';
  name.textContent = topic;
  row.appendChild(name);

  if (!isSubTopic) {
    const unsubBtn = document.createElement('button');
    unsubBtn.className = 'icon';
    unsubBtn.dataset.unsub = topic;
    unsubBtn.title = 'Unsubscribe';
    unsubBtn.innerHTML = '&times;';
    unsubBtn.addEventListener('click', () => deleteTopic(topic));
    row.appendChild(unsubBtn);
  }
  if (headTopic) {
    const headRow = topicList.querySelector(`[data-unsub="${escapeHtml(headTopic)}"]`)?.closest('.topic-row');
    if (headRow) {
      headRow.appendChild(row);
      return;
    }
  }
  topicList.appendChild(row);

  // MqttClients.get(currentClient).topics.push({ topic, subTopics: [] });
}

function addSubTopic(subTopic) {
  // search the current clients topic list to find the first that conforms to the message topic, with a pre check if that subtopic exists
  const currentTopicList = MqttClients.get(currentClient).topics;
  if (!currentTopicList) return;
  for (const topic of currentTopicList) {
    if (topic.subTopics.includes(subTopic)) {
      return;
    } else {
      addTopic(subTopic, true, topic);
      topic.subTopics.push(subTopic);
    }
  }
}

function setStatus(handle, state) {
  console.log("Setting status for handle:", handle, "state:", state);

  const statusIcon = document.querySelector('.statusIcon');
  if (statusIcon) {
    statusIcon.className = 'statusIcon' + (state === 'on' ? ' on' : state === 'off' ? ' off' : '');
  }
}

let pinned = true;

function appendMessage(m) {
  const stream = document.getElementById('stream');
  if (!stream) return;

  if (stream.childNodes.length > 1000) {
    stream.removeChild(stream.firstChild);
  }

  const div = document.createElement('div');
  const c = topicColor(m.topic);
  addSubTopic(m.topic);
  const open = openRows.has(m.id);
  const pretty = (() => {
    const o = tryParse(m.payload);
    return o === null ? m.payload : JSON.stringify(o, null, 2);
  })();
  div.innerHTML = `<div class="msg ${open ? 'open' : ''} ${m.out ? 'out' : ''}" data - id="${m.id}" style = "color:${c}" >
        <span class="m-time">${stamp(m.ts)}</span>
        <span class="m-line">
          <span class="m-topic">${escapeHtml(m.topic)}</span>
          <span class="m-peek">${escapeHtml(peek(m.payload))}</span>
        </span>
        <span class="flags">
          ${m.retain ? '<span class="flag r">R</span>' : ''}
          <span class="flag q">Q${m.qos}</span>
        </span>
        ${open ? `<div class="m-body">${highlight(pretty)}</div>` : ''}
      </div > `;
  stream.appendChild(div);
}

function push(m) {
  m.id = ++seq;
  appendMessage(m);
  if (pinned && stream) stream.scrollTop = stream.scrollHeight;
}

async function createClient(host, port) {
  console.log("Creating client with host:", host, "port:", port);
  let handle = await invoke('mqtt_connect', { ip: host, port: Number(port) });
  let clientInfo = await invoke('is_connected', { handle });
  clientInfo['topics'] = [];
  console.log("Created client with handle:", handle, "client info:", clientInfo);
  MqttClients.set(handle, clientInfo);
  setStatus(handle, 'on');
  return handle;
}


// ── Initialization ────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  stream = document.getElementById('stream');
  filterEl = document.getElementById('filter');
  jumpBtn = document.getElementById('jump');

  stream.addEventListener('scroll', () => {
    pinned = stream.scrollHeight - stream.scrollTop - stream.clientHeight < 40;
    jumpBtn.classList.toggle('show', !pinned);
  });
  jumpBtn.onclick = () => {
    stream.scrollTop = stream.scrollHeight;
    pinned = true;
    jumpBtn.classList.remove('show');
  };

  stream.addEventListener('click', e => {
    const row = e.target.closest('.msg');
    if (!row) return;
    const id = +row.dataset.id;
    openRows.has(id) ? openRows.delete(id) : openRows.add(id);
    renderStream();
  });

  filterEl.addEventListener('input', renderStream);
  document.getElementById('clearBtn').onclick = () => {
    messages = [];
    openRows.clear();
    renderStream();
  };

  function makeTab(text, host, port, handle) {
    console.log("Creating tab with text:", text, "host:", host, "port:", port);
    const tabContainer = document.getElementById('host-input-wrap');
    const div = document.createElement('div');
    const span = document.createElement('span');
    div.classList.add('active');
    span.textContent = text;
    div.dataset.host = host;
    div.dataset.port = port;

    div.addEventListener('click', () => {
      for (const d of tabContainer.getElementsByTagName('div')) {
        d.classList.remove('active');
      }
      div.classList.add('active');
    });

    const closeBtn = document.createElement('div');
    closeBtn.innerHTML = closeSvg;
    closeBtn.classList.add('closeBtn');
    closeBtn.addEventListener('click', async e => {
      e.stopPropagation();
      console.log("Disconnecting handle:", handle);
      await invoke('mqtt_disconnect', { handle: handle });
      connected = false;
      setStatus('off', 'Offline');
      tabContainer.removeChild(div);
      MqttClients.delete(handle);
      console.log("after remove", MqttClients);
      currentClient = null;
    });

    const statusIcon = document.createElement('div');
    statusIcon.classList.add('statusIcon');




    div.appendChild(statusIcon);
    div.appendChild(span);
    div.appendChild(closeBtn);
    tabContainer.appendChild(div);
  }

  function findActiveTab() {
    const tabContainer = document.getElementById('host-input-wrap');
    return tabContainer.querySelector('div.active');
  }

  function getTab(text, host, port) {
    const tabContainer = document.getElementById('host-input-wrap');
    return Array.from(tabContainer.getElementsByTagName('div')).find(div =>
      div.textContent === text && div.dataset.host === host && div.dataset.port === port
    );
  }

  document.getElementById('addHostBtn').onclick = async () => {

    const tailscalePeers = await tail();
    const result = await promptEditConnection(host, tailscalePeers);
    if (!result) return; // User cancelled the prompt
    console.log("User entered new connection:", result);
    host = parseHostPort(result);
    console.log("Updated host to:", host);
    let handle = await createClient(host.name, host.port);
    makeTab(result, host.name, host.port, handle);
    setStatus(handle, 'on');
    currentClient = handle;
    await store.set('mqttClients', MqttClients);

    console.log("after adding:", MqttClients);
  };

  const hostInputWrap = document.getElementById('host-input-wrap');
  for (const div of hostInputWrap.getElementsByTagName('div')) {
    div.addEventListener('click', () => {
      for (const d of hostInputWrap.getElementsByTagName('div')) {
        d.classList.remove('active');
      }
      div.classList.add('active');
    });
  }

  // document.getElementById('connectBtn').onclick = doConnect;

  document.getElementById('subBtn').onclick = subscribe;
  document.getElementById('subInput').addEventListener('keydown', e => {
    if (e.key === 'Enter') subscribe();
  });

  async function subscribe() {
    const el = document.getElementById('subInput');
    const t = el.value.trim();
    try { await invoke('mqtt_subscribe', { handle: currentClient, topic: t }); } catch (err) { return setStatus('err', String(err)); }
    el.value = '';
    MqttClients.get(currentClient).topics.push({ topic: t, subTopics: [] });
    addTopic(t);
  }

  document.querySelectorAll('#qos button').forEach(b => {
    b.onclick = () => {
      qos = +b.dataset.q;
      document.querySelectorAll('#qos button').forEach(x =>
        x.setAttribute('aria-pressed', String(+x.dataset.q === qos)));
    };
  });

  document.getElementById('formatBtn').onclick = () => {
    const el = document.getElementById('pubPayload');
    const o = tryParse(el.value);
    if (o !== null) el.value = JSON.stringify(o, null, 2);
  };

  document.getElementById('pubBtn').onclick = publish;
  document.getElementById('pubPayload').addEventListener('keydown', e => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) publish();
  });

  async function publish() {
    const topic = document.getElementById('pubTopic').value.trim();
    const payload = document.getElementById('pubPayload').value;
    const retain = document.getElementById('retain').checked;
    if (!topic) return;
    try {
      await invoke('mqtt_publish', { topic, payload, qos, retain });
      push({ ts: Date.now(), topic, payload, retain, qos, out: true });
    } catch (err) {
      setStatus('err', String(err));
    }
  }



  // ── Initial render & effect setup ──────────────────────────────────
  renderStream();

  await initLiquidGlass();

  if (T?.core?.invoke) {
    try {
      const clients = await invoke('get_all_clients');
      if (clients && clients.length > 0) {
        for (const client of clients) {
          if (client.connected) {
            console.log("Connected to MQTT broker:", client);
            makeTab(client.ip, client.ip, client.port, client.handle);
            setStatus(client.handle, 'on');

            // get all topics 
            try {
              const existingTopics = await invoke('get_subscribe_list', { handle: client.handle });
              if (Array.isArray(existingTopics)) {
                existingTopics.forEach(t => {
                  addTopic(t);
                  client["topics"] = [
                    { topic: t, subTopics: [] }
                  ];
                });
              }
            } catch (err) {
              console.error("Failed to get subscribe list for client:", client, err);
            }

            MqttClients.set(client.handle, client);
            console.log("Added client to MqttClients:", MqttClients);


          }
        }
        currentClient = clients[0].handle;
      }


      await loadStore();
      await store.set('currentClient', currentClient);
      await store.set('mqttClients', MqttClients);

      console.log("currentClient", currentClient);
    } catch (err) {
      console.error("Failed to get all clients:", err);
    }


    // ── Events from Rust ──────────────────────────────────────────────
    listen('mqtt-message', e => {
      if (currentClient == null) return;
      const { topic, payload, retain, qos } = e.payload;
      push({ ts: Date.now(), topic, payload, retain, qos, out: false });
    });
    listen('mqtt-error', e => { setStatus('err', String(e.payload)); });
  }

  console.log(await store.get('mqttClients'));
});





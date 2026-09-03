const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;
const { getCurrentWindow } = window.__TAURI__.window;

const messages = {

}

function addToSubList(topic) {
  const subscribeList = document.getElementById("subscribeList");
  const item = document.createElement("div");
  item.textContent = topic;
  subscribeList.appendChild(item);
}

function clearSubList() {
  const subscribeList = document.getElementById("subscribeList");
  subscribeList.innerHTML = "";
}

function makeMessageItem(topic, data, retain, qos) {
  const item = document.createElement("div");
  item.classList.add("message-item");
  const keys = Object.keys(data);
  for (const key of keys) {
    const value = data[key];
    const line = document.createElement("div");
    line.textContent = `${key}: ${JSON.stringify(value)} `;
    item.appendChild(line);
  }
  return item;
}

window.addEventListener("DOMContentLoaded", () => {
  void (async () => {
    const supported = await invoke("plugin:liquid-glass|is_glass_supported");
    const connectForm = document.getElementById("connectForm");
    const subscribeForm = document.getElementById("subscribeForm");

    if (!supported) {
      console.warn("Liquid Glass is not supported on this system.");
      return;
    }

    const appWindow = getCurrentWindow();

    await invoke("plugin:liquid-glass|set_liquid_glass_effect", {
      window: appWindow.label,
      config: {

        cornerRadius: 24,
      },
    });

    listen('mqtt-message', (e) => {
      const { topic, payload, retain, qos } = e.payload;
      const id = e.id;
      const data = JSON.parse(payload);
      console.log("Received MQTT message:", id, topic, data, retain, qos);
      if (!messages[topic]) {
        messages[topic] = [];
      }
      messages[topic].push({ id, data, retain, qos });
      const messageList = document.getElementById("messageList");
      const messageItem = makeMessageItem(topic, data, retain, qos);
      messageList.appendChild(messageItem);
    });


    connectForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const hostPort = document.querySelector("input[type=text]").value;
      // default port 1883 if not specified
      const [host, port = "1883"] = hostPort.split(":");
      const response = await invoke("mqtt_connect", { host, port: Number(port) });
      console.log(response);
    });

    subscribeForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const topic = document.querySelector("input[placeholder='mqtt topic']").value;
      const response = await invoke("mqtt_subscribe", { topic });
      console.log(response);
      addToSubList(topic);
    });
  })();
});

const isConnectedBtn = document.getElementById("isConnectedBtn");
const getSubscribeListBtn = document.getElementById("getSubscribeListBtn");

isConnectedBtn.addEventListener("click", async () => {
  const connected = await invoke("is_connected");
  console.log("Is connected:", connected);
});

getSubscribeListBtn.addEventListener("click", async () => {
  const list = await invoke("get_subscribe_list");
  clearSubList();
  list.forEach(addToSubList);
  console.log("Subscribe list:", list);
});

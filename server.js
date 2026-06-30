const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mqtt = require("mqtt");
const { createProxyMiddleware } = require("http-proxy-middleware");

// ── Config ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const FLASK_PORT = process.env.FLASK_PORT || 5005;
const MQTT_BROKER = process.env.MQTT_BROKER || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const MQTT_DISABLED = process.env.MQTT_DISABLED === "1" || process.env.MQTT_DISABLED === "true";
const MQTT_URL = `mqtt://${MQTT_BROKER}:${MQTT_PORT}`;
const FLASK_URL = `http://localhost:${FLASK_PORT}`;

const MAX_HISTORY = 50;
const sensorHistory = [];
const latest = {
  ph: null, temp: null, turbidity: null,
  light_lux: null, distance_cm: null, health: null,
  pump: "OFF", air_pump: "OFF", peltier: "OFF", rgb: "OFF", feeder: "STANDBY",
  filter_pump: "OFF", rgb_color: "blue", rgb_brightness: 60,
  gsm_status: "IDLE", mode: "AUTO",
  connection_source: "Offline", mqtt_connected: false,
  serial_connected: false, camera_connected: false,
  serial_raw: "No ESP32 data yet",
  updated_at: null,
};

function getTimestamp() {
  return new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function calcHealth() {
  let score = 100;
  if (latest.ph != null) {
    if (latest.ph < 6.0 || latest.ph > 8.0) score -= 30;
    else if (latest.ph < 6.5 || latest.ph > 7.5) score -= 15;
  }
  if (latest.temp != null) {
    if (latest.temp < 18 || latest.temp > 32) score -= 30;
    else if (latest.temp < 22 || latest.temp > 28) score -= 15;
  }
  if (latest.turbidity != null) {
    if (latest.turbidity > 3000) score -= 30;
    else if (latest.turbidity > 2000) score -= 15;
  }
  latest.health = Math.max(0, Math.min(100, score));
}

function pushSensorReading(payload) {
  Object.assign(latest, payload, { updated_at: Date.now() });
  calcHealth();
  const entry = { time: getTimestamp(), ...payload, health: latest.health };
  sensorHistory.push(entry);
  if (sensorHistory.length > MAX_HISTORY) sensorHistory.shift();
}

function parseStatus(payload) {
  const parts = payload.split("|");
  return { value: parts[0], mode: parts.length > 1 ? parts[1] : null };
}

function buildSnapshot() {
  return {
    ...latest,
    history: [...sensorHistory],
  };
}

// ── Express + Socket.IO ────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.get("/", (req, res) => res.sendFile("dashboard.html", { root: "frontend" }));
app.use(express.static("frontend"));
app.use(express.json());

// Proxy camera feed to Python Flask server
app.use("/camera/feed", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/camera", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/api/detections", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/ngrok", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/history", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/sensor_history", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/actuator_history", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/sms_history", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/latest", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/esp32", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));

// ── API endpoint (Node.js handles this directly — publishes MQTT) ─────────────
app.get("/api/data", (req, res) => {
  res.json(buildSnapshot());
});

app.post("/api/control", (req, res) => {
  const data = req.body || {};
  const commands = [];

  if (data.mode) {
    const mode = String(data.mode).toUpperCase();
    if (["AUTO", "MANUAL"].includes(mode)) {
      latest.mode = mode;
      for (const act of ["airpump", "pump", "cooling", "led", "feeder"]) {
        publish(`crayfish/mode/${act}`, mode);
      }
      commands.push(`MODE=${mode}`);
    }
  }

  const ACTUATOR_MAP = {
    pump: "crayfish/cmd/pump", air_pump: "crayfish/cmd/airpump",
    peltier: "crayfish/cmd/cooling", rgb: "crayfish/cmd/led",
  };
  for (const [key, topic] of Object.entries(ACTUATOR_MAP)) {
    if (data[key] !== undefined) {
      const val = data[key] === true ? "ON" : data[key] === false ? "OFF" : String(data[key]).toUpperCase();
      if (["ON", "OFF"].includes(val)) {
        latest[key] = val;
        publish(topic, val);
        commands.push(`${key}=${val}`);
      }
    }
  }

  if (data.feeder === "FEED") {
    publish("crayfish/cmd/feeder", "FEED");
    latest.feeder = "FEED";
    commands.push("feeder=FEED");
  }

  // RGB and GSM — just forward to Flask for MongoDB logging
  if (data.rgb_color || data.rgb_brightness || data.gsm_alert || data.stepper_rotate) {
    const FLASK_PORT_CTRL = process.env.FLASK_PORT || 5005;
    fetch(`http://localhost:${FLASK_PORT_CTRL}/api/control`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    }).catch(() => {});
  }

  const snapshot = buildSnapshot();
  io.emit("sensor_update", snapshot);
  res.json({ status: "ok", sent: commands, state: snapshot });
});

// ── MQTT Client ────────────────────────────────────────────────────────────────
let mqttClient = null;

function publish(topic, payload) {
  if (mqttClient && mqttClient.connected) {
    mqttClient.publish(topic, payload);
  }
}

if (!MQTT_DISABLED) {
  mqttClient = mqtt.connect(MQTT_URL, { connectTimeout: 5000, reconnectPeriod: 10000 });

  mqttClient.on("connect", () => {
    console.log(`MQTT connected to ${MQTT_BROKER}:${MQTT_PORT}`);
    latest.mqtt_connected = true;
    latest.connection_source = "MQTT";
    mqttClient.subscribe("crayfish/#", (err) => {
      if (err) console.error("MQTT subscribe error:", err);
      else console.log("Subscribed to crayfish/#");
    });
  });

  mqttClient.on("message", (topic, message) => {
    const payload = message.toString().trim();
    const parts = topic.split("/");
    const base = parts[parts.length - 1];
    const prefix = parts.length >= 3 ? parts[parts.length - 2] : "";
    let data = {};

    // Sensor values (ESP32 publishes plain numbers)
    if (base === "ph") data.ph = parseFloat(payload);
    else if (base === "turbidity") data.turbidity = parseInt(payload, 10);
    else if (base === "distance") data.distance_cm = parseFloat(payload);
    else if (base === "lux") data.light_lux = parseFloat(payload);
    else if (base === "temperature") data.temp = parseFloat(payload);
    else if (base === "alert") data.alert = payload;

    // Status topics: "crayfish/status/{actuator}" -> payload = "ON|AUTO"
    else if (prefix === "status") {
      const s = parseStatus(payload);
      if (base === "airpump") { data.air_pump = s.value; if (s.mode) data.mode = s.mode; }
      else if (base === "pump") { data.pump = s.value; if (s.mode) data.mode = s.mode; }
      else if (base === "cooling") { data.peltier = s.value; if (s.mode) data.mode = s.mode; }
      else if (base === "led") { data.rgb = s.value; if (s.mode) data.mode = s.mode; }
      else if (base === "feeder") data.feeder = s.value;
      else if (base === "gsm") data.gsm_status = s.value;
    }

    else if (topic === "crayfish/rgb/color") data.rgb_color = payload.toLowerCase();
    else if (topic === "crayfish/rgb/brightness") data.rgb_brightness = parseInt(payload, 10);

    if (Object.keys(data).length > 0) {
      pushSensorReading(data);
      io.emit("sensor_update", buildSnapshot());
    }
  });

  mqttClient.on("error", (err) => {
    console.error("MQTT error:", err.message);
    latest.mqtt_connected = false;
  });

  mqttClient.on("close", () => {
    latest.mqtt_connected = false;
  });
} else {
  console.log("MQTT disabled (MQTT_DISABLED=1) — no real-time sensor data");
}

// ── Socket.IO events ──────────────────────────────────────────────────────────
io.on("connection", (socket) => {
  console.log(`[SocketIO] Client connected (${socket.id})`);
  socket.emit("sensor_update", buildSnapshot());

  socket.on("disconnect", () => {
    console.log(`[SocketIO] Client disconnected (${socket.id})`);
  });
});

// ── Start ──────────────────────────────────────────────────────────────────────
server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n  Crayfish IoT — Node.js Web Server`);
  console.log(`  Dashboard : http://localhost:${PORT}`);
  console.log(`  MQTT      : ${MQTT_URL} → crayfish/#`);
  console.log(`  Python    : ${FLASK_URL} (camera, AI, serial, MongoDB)\n`);
  console.log(`  npm start runs BOTH Node.js and Python side-by-side.`);
  console.log(`  The Python app.py handles camera, AI detection, serial,`);
  console.log(`  MongoDB, ngrok. Node.js handles real-time web serving.\n`);
});

// ── Simulation endpoint (testing without ESP32) ───────────────────────────────
let simInterval = null;
app.post("/api/simulate/start", (req, res) => {
  if (simInterval) clearInterval(simInterval);
  const interval = (req.body && req.body.interval) || 2000;
  function push() {
    const r = {
      ph: Math.round((Math.random() * 1.5 + 6.5) * 10) / 10,
      temp: Math.round((Math.random() * 6 + 22) * 10) / 10,
      turbidity: Math.round(Math.random() * 2000 + 500),
      light_lux: Math.round(Math.random() * 550 + 50),
      distance_cm: Math.round((Math.random() * 40 + 10) * 10) / 10,
    };
    pushSensorReading(r);
    io.emit("sensor_update", buildSnapshot());
  }
  push();
  simInterval = setInterval(push, interval);
  res.json({ status: "simulation_started", interval });
});
app.post("/api/simulate/stop", (req, res) => {
  if (simInterval) clearInterval(simInterval);
  simInterval = null;
  res.json({ status: "simulation_stopped" });
});
app.get("/api/simulate/status", (req, res) => {
  res.json({ running: simInterval !== null });
});

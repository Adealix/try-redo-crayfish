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
  // Matches Python helpers.py:compute_health exactly
  let score = 100;
  if (latest.ph != null) score -= Math.abs(latest.ph - 7.0) * 18;
  else score -= 20;
  if (latest.temp != null) score -= Math.abs(latest.temp - 27.0) * 10;
  else score -= 20;
  latest.health = Math.max(0, Math.min(100, Math.round(score)));
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
// Member pages (proxied to Flask)
app.use("/dashboard", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/maranan", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/calungsod", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/garcia", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/canta", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/delrosario", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));
app.use("/famini", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));

// ── API — proxy /api/data to Flask for rich response (detection, history, events) ──
app.use("/api/data", createProxyMiddleware({ target: FLASK_URL, changeOrigin: true }));

app.post("/api/control", (req, res) => {
  const data = req.body || {};
  const commands = [];
  const rejected = [];

  // Matches Python app.py ESP32_ACTUATOR mapping exactly
  const ACTUATOR_MAP = {
    pump:     { topic: "crayfish/cmd/pump",     serialKey: "PUMP" },
    air_pump: { topic: "crayfish/cmd/airpump",  serialKey: "AIR_PUMP" },
    peltier:  { topic: "crayfish/cmd/cooling",  serialKey: "PELTIER" },
    rgb:      { topic: "crayfish/cmd/led",      serialKey: "RGB" },
  };

  const effectiveMode = latest.mode || "AUTO";
  const manualActive = effectiveMode === "MANUAL";

  // ── Mode ──────────────────────────────────────────────────────────────
  if (data.mode) {
    const mode = String(data.mode).toUpperCase();
    if (["AUTO", "MANUAL"].includes(mode)) {
      latest.mode = mode;
      for (const act of ["airpump", "pump", "cooling", "led"]) {
        publish(`crayfish/mode/${act}`, mode);
      }
      commands.push(`MODE=${mode}`);
    }
  }

  // ── Relays (OFF allowed in AUTO for safety; ON/TOGGLE needs MANUAL) ──
  for (const [key, cfg] of Object.entries(ACTUATOR_MAP)) {
    if (data[key] !== undefined) {
      const raw = data[key];
      const val = raw === true ? "ON" : raw === false ? "OFF" : String(raw).toUpperCase();

      if (!manualActive && val !== "OFF") {
        rejected.push(key);
        continue;
      }

      if (val === "ON" || val === "OFF") {
        latest[key] = val;
        publish(cfg.topic, val);
        commands.push(`${key}=${val}`);
      }
    }
  }

  // ── Feeder ────────────────────────────────────────────────────────────
  if (data.feeder === "FEED") {
    if (!manualActive) {
      rejected.push("feeder");
    } else {
      publish("crayfish/cmd/feeder", "FEED");
      commands.push("feeder=FEED");
    }
  }

  // ── RGB colour (matches Python validation) ────────────────────────────
  if (data.rgb_color !== undefined) {
    if (!manualActive) {
      rejected.push("rgb_color");
    } else {
      const colour = String(data.rgb_color).toLowerCase();
      const valid = ["blue", "cyan", "purple", "white", "red", "green", "yellow"];
      if (valid.includes(colour)) {
        latest.rgb_color = colour;
        publish("crayfish/cmd/rgb_color", colour.toUpperCase());
        commands.push(`rgb_color=${colour}`);
      }
    }
  }

  // ── RGB brightness (clamped 0–100, matches Python) ───────────────────
  if (data.rgb_brightness !== undefined) {
    if (!manualActive) {
      rejected.push("rgb_brightness");
    } else {
      const brightness = Math.max(0, Math.min(100, parseInt(data.rgb_brightness, 10) || 0));
      latest.rgb_brightness = brightness;
      publish("crayfish/cmd/rgb_brightness", String(brightness));
      commands.push(`rgb_brightness=${brightness}`);
    }
  }

  // ── GSM alert (not gated by mode — matches Python) ───────────────────
  if (data.gsm_alert) {
    latest.gsm_status = "SENDING";
    commands.push(`GSM_ALERT=${data.gsm_alert}`);
    publish("crayfish/cmd/gsm_alert", String(data.gsm_alert));
  }

  // ── Stepper (forward to Flask for camera/detection path) ─────────────
  if (data.stepper_rotate) {
    commands.push(`STEPPER_ROTATE ${data.stepper_direction || "CW"} x${data.stepper_rotations || 2}`);
    fetch(`http://localhost:${FLASK_PORT}/api/control`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    }).catch(() => {});
  }

  const snapshot = buildSnapshot();
  io.emit("sensor_update", snapshot);

  const response = { status: "ok", sent: commands, state: snapshot };
  if (rejected.length > 0) {
    response.status = commands.length > 0 ? "partial" : "ignored";
    response.rejected = rejected;
    response.reason = "System is in AUTO mode — switch to Manual to control actuators.";
  }
  res.json(response);
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
      else if (base === "filter_pump") data.filter_pump = s.value;
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

// ── Socket.IO — real-time push (no polling) ─────────────────────────────────
const socket = io({ transports: ["websocket"], reconnection: true });
let chart = null;

function fmt(v, d, s) { return v != null ? Number(v).toFixed(d) + s : "--"; }

function applyState(d) {
  document.getElementById("tempValue").textContent    = fmt(d.temp, 1, "°C");
  document.getElementById("phValue").textContent      = fmt(d.ph, 2, "");
  document.getElementById("turbidityValue").textContent = fmt(d.turbidity, 0, "");
  document.getElementById("luxValue").textContent     = fmt(d.light_lux, 0, "");
  document.getElementById("distanceValue").textContent = fmt(d.distance_cm, 1, "cm");
  document.getElementById("healthValue").textContent  = fmt(d.health, 0, "%");

  const online = d.connection_source === "MQTT" || d.mqtt_connected;
  document.getElementById("connDot").style.background = online ? "#22c55e" : "#ef4444";
  document.getElementById("connLabel").textContent = online ? "Connected" : "Offline";
  document.getElementById("mqttStatus").textContent = d.mqtt_connected ? "✓ Connected" : "✗ Disconnected";
  document.getElementById("serialStatus").textContent = d.serial_connected ? "✓ Connected" : "✗ Disconnected";
  document.getElementById("cameraStatus").textContent = d.camera_connected ? "✓ Online" : "✗ Offline";
  document.getElementById("systemMode").textContent = d.mode || "AUTO";

  for (const k of ["pump", "air_pump", "peltier", "rgb"]) {
    const el = document.getElementById(k + "Badge");
    if (el) el.textContent = d[k] || "OFF";
  }
  const fb = document.getElementById("feederBadge");
  if (fb) fb.textContent = d.feeder || "STANDBY";
  const mb = document.getElementById("modeBadge");
  if (mb) mb.textContent = d.mode || "AUTO";

  // Chart
  if (d.history && d.history.length > 1) {
    const labels = d.history.map(r => r.time || "");
    const ctx = document.getElementById("sensorChart").getContext("2d");
    if (chart) chart.destroy();
    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          { label: "pH", data: d.history.map(r => r.ph), tension: 0.3, borderColor: "#8b6df8", backgroundColor: "rgba(139,109,248,0.12)", fill: true, yAxisID: "y", pointRadius: 2 },
          { label: "Temp °C", data: d.history.map(r => r.temp), tension: 0.3, borderColor: "#22c55e", backgroundColor: "rgba(34,197,94,0.10)", fill: true, yAxisID: "y1", pointRadius: 2 },
          { label: "Turbidity", data: d.history.map(r => r.turbidity), tension: 0.3, borderColor: "#f59e0b", backgroundColor: "rgba(245,158,11,0.10)", fill: true, yAxisID: "y2", pointRadius: 2 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "top", labels: { usePointStyle: true, boxWidth: 8, color: "#374151", font: { size: 11 } } } },
        scales: {
          x: { grid: { display: false }, ticks: { color: "#6b7280", font: { size: 10 }, maxTicksLimit: 6 } },
          y: { type: "linear", position: "left", min: 0, max: 14, ticks: { color: "#6b7280", font: { size: 10 } }, grid: { color: "rgba(17,24,39,0.06)" } },
          y1: { type: "linear", position: "right", min: 0, max: 40, ticks: { color: "#6b7280", font: { size: 10 } }, grid: { drawOnChartArea: false } },
          y2: { type: "linear", position: "right", min: 0, max: 4095, ticks: { display: false }, grid: { drawOnChartArea: false } },
        },
      },
    });
  }
}

// Real-time WebSocket push
socket.on("sensor_update", applyState);

// Initial load via REST
fetch("/api/data").then(r => r.json()).then(applyState).catch(() => {});

// Clock
function tick() { document.getElementById("clock").textContent = new Date().toLocaleTimeString(); }
setInterval(tick, 1000); tick();

// Greeting
const h = new Date().getHours();
document.getElementById("greeting").textContent =
  h < 12 ? "Good morning, Operator!" : h < 18 ? "Good afternoon, Operator!" : "Good evening, Operator!";

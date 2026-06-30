import time
import paho.mqtt.client as mqtt

import state as S
from state import state_lock
from helpers import compute_health, log_event, now_hms
from config import MQTT_ENABLED, MQTT_BROKER, MQTT_PORT, MQTT_BASE_TOPIC
from db import mongo_logger

# Module-level client reference used by `publish`
_client = None

# Track previous gsm_status so we only log SMS events on actual changes
_prev_gsm = None


def on_message(client, userdata, msg):
    global _prev_gsm
    topic = msg.topic
    try:
        value = msg.payload.decode().strip()
    except Exception:
        return

    # ── 1. Update shared state (under lock) ──────────────────────────────────
    with state_lock:
        try:
            if topic == "crayfish/temperature":
                S.state["temp"] = float(value)
            elif topic == "crayfish/ph":
                S.state["ph"] = float(value)
            elif topic == "crayfish/turbidity":
                S.state["turbidity"] = float(value)
            elif topic == "crayfish/lux":
                S.state["light_lux"] = float(value)
            elif topic == "crayfish/distance":
                S.state["distance_cm"] = float(value)
            elif topic == "crayfish/status/pump":
                S.state["pump"] = value.split("|")[0]
            elif topic == "crayfish/status/cooling":
                S.state["peltier"] = value.split("|")[0]
            elif topic == "crayfish/status/airpump":
                S.state["air_pump"] = value.split("|")[0]
            elif topic == "crayfish/status/filter_pump":
                S.state["filter_pump"] = value.split("|")[0]
            elif topic == "crayfish/status/led":
                led_state = value.split("|")[0]
                S.state["led"] = led_state
                # The dashboard's "RGB Strip" control maps to this physical LED actuator
                S.state["rgb"] = led_state
            elif topic == "crayfish/rgb/color":
                S.state["rgb_color"] = value.lower()
            elif topic == "crayfish/rgb/brightness":
                S.state["rgb_brightness"] = int(float(value))
            elif topic == "crayfish/status/gsm":
                S.state["gsm_status"] = value
            elif topic == "crayfish/alert":
                log_event("alert", "Crayfish Alert", value)

            S.state["health"]           = compute_health(S.state["ph"], S.state["temp"])
            S.state["updated_at"]       = time.time()
            S.state["mqtt_connected"]   = True
            S.state["connection_source"] = "MQTT"

        except Exception as e:
            print(f"[MQTT] Error processing {topic}: {e}")
            return

        # Build snapshots while still holding the lock (consistent read)
        tick_time = now_hms()
        tick_ts   = time.time()

        sensor_snapshot = {
            "time":        tick_time,
            "ts":          tick_ts,
            "ph":          S.state["ph"],
            "temp":        S.state["temp"],
            "turbidity":   S.state["turbidity"],
            "light_lux":   S.state["light_lux"],
            "distance_cm": S.state["distance_cm"],
            "health":      S.state["health"],
        }

        actuator_snapshot = {
            "time":           tick_time,
            "ts":             tick_ts,
            "pump":           S.state["pump"],
            "peltier":        S.state["peltier"],
            "air_pump":       S.state["air_pump"],
            "filter_pump":    S.state["filter_pump"],
            "rgb":            S.state.get("rgb"),
            "rgb_color":      S.state["rgb_color"],
            "rgb_brightness": S.state["rgb_brightness"],
            "mode":           S.state.get("mode"),
        }

        current_gsm = S.state["gsm_status"]
        gsm_changed = current_gsm and current_gsm != _prev_gsm

        # Update in-memory history deque (offline fallback for dashboard)
        S.history.append({
            **sensor_snapshot,
            **actuator_snapshot,
            "gsm_status": current_gsm,
        })

    # ── 2. Persist to MongoDB OUTSIDE state_lock ─────────────────────────────
    mongo_logger.insert_sensor(sensor_snapshot)
    mongo_logger.insert_actuator(actuator_snapshot)

    if gsm_changed:
        mongo_logger.insert_sms_event(status=current_gsm, detail=f"topic={topic} value={value}")
        _prev_gsm = current_gsm


def publish(topic, payload, qos=0, retain=False):
    if not MQTT_ENABLED:
        return False
    global _client
    try:
        if _client is None:
            return False
        _client.publish(topic, payload, qos=qos, retain=retain)
        return True
    except Exception as e:
        print(f"[MQTT] publish error: {e}")
        return False


def mqtt_worker():
    if not MQTT_ENABLED:
        print("[MQTT] Disabled via configuration.")
        return

    global _client
    _client = mqtt.Client()
    _client.on_message = on_message

    def on_connect(c, userdata, flags, rc):
        print(f"[MQTT] Connected (rc={rc})")
        try:
            c.subscribe(f"{MQTT_BASE_TOPIC}/#")
        except Exception:
            c.subscribe("crayfish/#")
        with state_lock:
            S.state["mqtt_connected"]    = True
            S.state["connection_source"] = "MQTT"

    def on_disconnect(c, userdata, rc):
        print(f"[MQTT] Disconnected (rc={rc})")
        with state_lock:
            S.state["mqtt_connected"] = False
            if S.state.get("serial_connected"):
                S.state["connection_source"] = "Serial"
            else:
                S.state["connection_source"] = "Offline"

    _client.on_connect    = on_connect
    _client.on_disconnect = on_disconnect

    while True:
        try:
            _client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            _client.loop_forever()
        except Exception as e:
            print(f"[MQTT] Connection failed: {e}. Retrying in 5s...")
            with state_lock:
                S.state["mqtt_connected"] = False
                if S.state.get("serial_connected"):
                    S.state["connection_source"] = "Serial"
                else:
                    S.state["connection_source"] = "Offline"
            time.sleep(5)


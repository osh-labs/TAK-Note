"""
OTS-Notehub-Plugin
==================
OpenTAKServer plugin that ingests Blues Wireless Notecard events from Notehub.io
and publishes them to connected ATAK/WinTAK/iTAK EUDs as Cursor-on-Target (CoT)
messages.

Data path (polling mode):
  Notecard → cellular → Notehub.io → REST API (events-cursor)
      → this plugin → RabbitMQ (cot_controller) → cot_parser → EUDs

Data path (webhook mode, optional):
  Notecard → cellular → Notehub.io → HTTP Route → /api/notehub/webhook
      → this plugin → RabbitMQ (cot_controller) → cot_parser → EUDs

CoT UID format:  Notehub-dev-<device-uid-without-colons>
  e.g.           Notehub-dev-5c0272311928
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from xml.etree.ElementTree import Element, SubElement, tostring

import pika
import requests
from flask import Blueprint, request, jsonify, current_app

from opentakserver.plugins.Plugin import Plugin

logger = logging.getLogger("OTS.NotehubPlugin")

NOTEHUB_API_BASE = "https://api.notefile.net/v1"
CURSOR_FILENAME = "notehub_cursor.txt"

# ---------------------------------------------------------------------------
# Module-level blueprint — routes must be defined at module scope so they are
# registered before the plugin class is instantiated.
# ---------------------------------------------------------------------------

blueprint = Blueprint(
    "ots_notehub_plugin",
    __name__,
    url_prefix="/api/notehub",
)


# ---------------------------------------------------------------------------
# CoT helper functions (module-level so both poll loop and webhook can use them)
# ---------------------------------------------------------------------------

def _resolve_location(event: dict) -> tuple[float | None, float | None, str, str]:
    """
    Return (lat, lon, ce_metres_str, location_type) from a Notehub event dict.

    Priority:
      1. best_lat / best_lon  (Notehub picks the most accurate available source)
      2. where_lat / where_lon  (raw GPS if available)
      3. tri_lat / tri_lon      (cell-tower / WiFi triangulation)
      4. tower_lat / tower_lon  (single cell tower centroid)

    CE (circular error, metres) is set conservatively per location type:
      gps            →  50 m   (typical Notecard GNSS accuracy)
      triangulated   →  2 000 m
      tower          →  9 999 999 (unknown / worst-case)
    """
    location_type = event.get("best_location_type", "tower")

    lat = event.get("best_lat")
    lon = event.get("best_lon")

    # Fallback chain if best_* fields are absent
    if lat is None:
        lat = event.get("where_lat") or event.get("tri_lat") or event.get("tower_lat")
    if lon is None:
        lon = event.get("where_lon") or event.get("tri_lon") or event.get("tower_lon")

    if lat is None or lon is None:
        return None, None, "9999999", location_type

    ce_map = {
        "gps": "50",
        "triangulated": "2000",
        "tower": "9999999",
    }
    ce = ce_map.get(location_type, "9999999")

    return float(lat), float(lon), ce, location_type


def _fmt(dt: datetime) -> str:
    """Format a datetime as a CoT timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _event_to_cot_xml(event: dict, cot_type: str, stale_seconds: int) -> str | None:
    """
    Convert a Notehub event dict to a CoT XML string.

    Returns None if the event has no usable coordinates.

    Body field mapping:
      body.alt / body.altitude  →  <point hae="...">
      body.speed                →  <track speed="...">
      body.course / body.heading→  <track course="...">
      All other body fields     →  <remarks>key=value; ...</remarks>

    Battery: event.voltage (volts) is mapped to approximate percentage
    using a linear scale (3.0 V = 0 %, 4.2 V = 100 %).
    """
    lat, lon, ce, location_type = _resolve_location(event)
    if lat is None:
        logger.debug(
            "Notehub event %s has no usable coordinates — skipping",
            event.get("event", "?"),
        )
        return None

    # --- Identity ---
    device_uid = event.get("device", str(uuid.uuid4()))
    callsign = (
        event.get("sn")
        or event.get("best_id")
        or device_uid
    )
    # Sanitise: strip protocol prefix and colons for use as XML attribute value
    cot_uid = "Notehub-" + device_uid.replace(":", "-")

    # --- Timestamps ---
    when_epoch = event.get("when") or event.get("received") or time.time()
    capture_dt = datetime.fromtimestamp(float(when_epoch), tz=timezone.utc)
    stale_dt = capture_dt + timedelta(seconds=stale_seconds)

    # --- Body payload ---
    body = event.get("body") or {}

    # HAE: prefer explicit altitude fields in body
    hae = str(float(body.get("alt", body.get("altitude", 0))))

    # --- Build XML tree ---
    event_elem = Element("event", {
        "version": "2.0",
        "uid": cot_uid,
        "type": cot_type,
        "how": "m-g",           # machine-generated (automated IoT device)
        "time": _fmt(capture_dt),
        "start": _fmt(capture_dt),
        "stale": _fmt(stale_dt),
    })

    SubElement(event_elem, "point", {
        "lat": str(lat),
        "lon": str(lon),
        "hae": hae,
        "ce": ce,
        "le": "9999999",
    })

    detail = SubElement(event_elem, "detail")

    SubElement(detail, "contact", {"callsign": callsign})
    SubElement(detail, "uid", {"Droid": callsign})

    # Track element (speed / course)
    speed = body.get("speed")
    course = body.get("course") or body.get("heading")
    if speed is not None or course is not None:
        track_attrs: dict[str, str] = {}
        if speed is not None:
            track_attrs["speed"] = str(speed)
        if course is not None:
            track_attrs["course"] = str(course)
        SubElement(detail, "track", track_attrs)

    # Battery status from Notehub session voltage field
    voltage = event.get("voltage")
    if voltage is not None and float(voltage) > 0:
        pct = int(max(0, min(100, (float(voltage) - 3.0) / 1.2 * 100)))
        SubElement(detail, "status", {"battery": str(pct)})

    # Remarks: remaining body fields that aren't already mapped
    MAPPED_BODY_KEYS = {"alt", "altitude", "speed", "course", "heading"}
    extra = {k: v for k, v in body.items() if k not in MAPPED_BODY_KEYS}
    if extra:
        remarks = SubElement(detail, "remarks")
        remarks.text = "; ".join(f"{k}={v}" for k, v in extra.items())

    # Flow tag so OTS can identify plugin-originated CoT in the database
    SubElement(
        detail,
        "_flow-tags_",
        {"OTS-Notehub-Plugin": _fmt(datetime.now(tz=timezone.utc))},
    )

    return tostring(event_elem, encoding="unicode")


def _publish_cot(uid: str, cot_xml: str, rabbit_host: str, ttl: str) -> None:
    """
    Publish a CoT message to the OTS internal RabbitMQ cot_controller exchange.

    The message format matches what eud_handler publishes when an EUD sends CoT:
        {"uid": "<device-uid>", "cot": "<CoT XML string>"}

    The cot_parser process picks this up, parses it, persists it to the database,
    and routes it to all connected EUDs via the 'cot' fanout exchange.
    """
    body = json.dumps({"uid": uid, "cot": cot_xml})
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_host, heartbeat=60)
        )
        channel = connection.channel()
        channel.basic_publish(
            exchange="cot_controller",
            routing_key="",
            body=body,
            properties=pika.BasicProperties(expiration=str(ttl)),
        )
        connection.close()
        logger.debug("CoT published for UID=%s", uid)
    except pika.exceptions.AMQPError as exc:
        logger.error("RabbitMQ publish failed for UID=%s: %s", uid, exc)


# ---------------------------------------------------------------------------
# Webhook endpoint (optional — for Notehub HTTP Routes push delivery)
# ---------------------------------------------------------------------------

@blueprint.route("/webhook", methods=["POST"])
def notehub_webhook():
    """
    Webhook endpoint for Notehub HTTP Route push delivery.

    When enabled, configure a Notehub HTTP Route to POST events to:
        https://<ots-host>/api/notehub/webhook

    Include the header:
        X-Notehub-Secret: <OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET>

    Notehub will POST either a single event JSON object or a JSON array.
    Each event with valid coordinates is immediately published as CoT.

    Note: this endpoint does NOT require OTS user authentication because
    incoming requests come from Notehub's servers, not from EUD users.
    Authentication is performed via the shared secret header instead.
    """
    config = current_app.config

    if not config.get("OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED", False):
        return jsonify({"error": "webhook endpoint not enabled"}), 404

    # Shared-secret validation
    expected = config.get("OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET", "")
    if expected:
        provided = request.headers.get("X-Notehub-Secret", "")
        if provided != expected:
            logger.warning("Notehub webhook: invalid or missing X-Notehub-Secret header")
            return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if not payload:
        return jsonify({"error": "request body must be valid JSON"}), 400

    events = payload if isinstance(payload, list) else [payload]

    cot_type = config.get("OTS_NOTEHUB_PLUGIN_COT_TYPE", "a-f-G-U-C")
    stale_seconds = int(config.get("OTS_NOTEHUB_PLUGIN_COT_STALE_TIME", 300))
    rabbit_host = config.get("OTS_RABBITMQ_SERVER_ADDRESS", "127.0.0.1")
    ttl = str(config.get("OTS_RABBITMQ_TTL", "86400000"))

    ingested = 0
    for ev in events:
        cot_xml = _event_to_cot_xml(ev, cot_type, stale_seconds)
        if cot_xml:
            uid = ev.get("best_id") or ev.get("device") or "unknown"
            _publish_cot(uid, cot_xml, rabbit_host, ttl)
            ingested += 1

    logger.info("Notehub webhook: ingested %d / %d events", ingested, len(events))
    return jsonify({"ingested": ingested, "received": len(events)}), 200


@blueprint.route("/status", methods=["GET"])
def notehub_status():
    """
    Simple status endpoint.  No auth required — returns non-sensitive config summary.
    Access at: GET /api/notehub/status
    """
    config = current_app.config
    return jsonify({
        "plugin": "OTS-Notehub-Plugin",
        "enabled": bool(config.get("OTS_NOTEHUB_PLUGIN_ENABLED", False)),
        "project_uid": config.get("OTS_NOTEHUB_PLUGIN_PROJECT_UID", ""),
        "poll_interval_s": config.get("OTS_NOTEHUB_PLUGIN_POLL_INTERVAL", 30),
        "cot_type": config.get("OTS_NOTEHUB_PLUGIN_COT_TYPE", "a-f-G-U-C"),
        "stale_seconds": config.get("OTS_NOTEHUB_PLUGIN_COT_STALE_TIME", 300),
        "notefile_filter": config.get("OTS_NOTEHUB_PLUGIN_NOTEFILE_FILTER", "(all)"),
        "webhook_enabled": bool(config.get("OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED", False)),
    }), 200


# ---------------------------------------------------------------------------
# Plugin class
# ---------------------------------------------------------------------------

class NotehubPlugin(Plugin):
    """
    OpenTAKServer plugin — Blues Wireless Notehub integration.

    Required config.yml keys (~/ots/config.yml):
        OTS_NOTEHUB_PLUGIN_ENABLED: true
        OTS_NOTEHUB_PLUGIN_API_KEY: "<notehub-personal-access-token>"
        OTS_NOTEHUB_PLUGIN_PROJECT_UID: "app:xxxx-xxxx-xxxx-xxxx"

    See default_config.py for all optional settings.
    """

    blueprint = blueprint

    def __init__(self):
        super().__init__()
        self._app = None
        self._config: dict = {}
        self._stop_event = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._cursor: str | None = None
        self._cursor_file: str | None = None
        self.metadata = self.load_metadata()

    # ------------------------------------------------------------------
    # Plugin.Plugin abstract interface
    # ------------------------------------------------------------------

    @property
    def group(self) -> str:
        return "opentakserver.plugins"

    def activate(self, app, enabled: bool) -> None:
        """
        Called by PluginManager at startup.

        If enabled=True and OTS_NOTEHUB_PLUGIN_ENABLED=True in config,
        starts the background polling thread.  The Flask blueprint is
        registered by PluginManager automatically — do not call
        app.register_blueprint() here.
        """
        self._app = app
        self._config = app.config

        ots_dir = os.path.expanduser("~/ots")
        self._cursor_file = os.path.join(ots_dir, CURSOR_FILENAME)
        self._load_cursor()

        if enabled and self._config.get("OTS_NOTEHUB_PLUGIN_ENABLED", False):
            self._stop_event.clear()
            self._poll_thread = threading.Thread(
                target=self._poll_loop,
                name="notehub_poller",
                daemon=True,
            )
            self._poll_thread.start()
            logger.info(
                "Notehub plugin activated: polling %s every %ds",
                self._config.get("OTS_NOTEHUB_PLUGIN_PROJECT_UID", "<no project>"),
                self._config.get("OTS_NOTEHUB_PLUGIN_POLL_INTERVAL", 30),
            )
        else:
            logger.info(
                "Notehub plugin loaded (enabled=%s, OTS_NOTEHUB_PLUGIN_ENABLED=%s) "
                "— polling not started",
                enabled,
                self._config.get("OTS_NOTEHUB_PLUGIN_ENABLED", False),
            )

    def stop(self) -> None:
        """Called by PluginManager on shutdown or disable."""
        self._stop_event.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=15)
        self._save_cursor()
        logger.info("Notehub plugin stopped")

    def get_info(self) -> dict | None:
        return {
            "name": "OTS-Notehub-Plugin",
            "polling_active": (
                self._poll_thread is not None and self._poll_thread.is_alive()
            ),
            "cursor": self._cursor,
            "project_uid": self._config.get("OTS_NOTEHUB_PLUGIN_PROJECT_UID", ""),
        }

    def load_metadata(self) -> dict:
        return {
            "name": "OTS-Notehub-Plugin",
            "description": (
                "Ingests Blues Wireless Notecard events from Notehub.io "
                "as CoT messages for ATAK/WinTAK/iTAK EUDs"
            ),
            "version": "1.0.0",
            "author": "Chris Lee / SEM-SEG",
            "url": "https://github.com/your-org/OTS-Notehub-Plugin",
        }

    # ------------------------------------------------------------------
    # Cursor persistence
    # ------------------------------------------------------------------

    def _load_cursor(self) -> None:
        """Read the previously persisted Notehub cursor from disk."""
        if not self._cursor_file:
            return
        try:
            if os.path.exists(self._cursor_file):
                with open(self._cursor_file, "r") as fh:
                    value = fh.read().strip()
                    self._cursor = value or None
                logger.debug("Loaded Notehub cursor: %s", self._cursor)
        except OSError as exc:
            logger.warning("Could not read cursor file %s: %s", self._cursor_file, exc)

    def _save_cursor(self) -> None:
        """Persist the current Notehub cursor to disk for next startup."""
        if not self._cursor_file or not self._cursor:
            return
        try:
            with open(self._cursor_file, "w") as fh:
                fh.write(self._cursor)
        except OSError as exc:
            logger.warning("Could not write cursor file %s: %s", self._cursor_file, exc)

    # ------------------------------------------------------------------
    # Background polling loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        """
        Background daemon thread.  Polls the Notehub events-cursor endpoint
        at OTS_NOTEHUB_PLUGIN_POLL_INTERVAL seconds and publishes any new
        events with valid coordinates as CoT messages.
        """
        interval = int(self._config.get("OTS_NOTEHUB_PLUGIN_POLL_INTERVAL", 30))
        logger.info("Notehub poll loop started, interval=%ds", interval)

        while not self._stop_event.is_set():
            try:
                self._fetch_and_publish()
            except requests.exceptions.RequestException as exc:
                logger.error("Notehub HTTP request failed: %s", exc)
            except Exception as exc:
                logger.error("Notehub poll error: %s", exc, exc_info=True)

            # Wait for the configured interval, but wake immediately on stop
            self._stop_event.wait(timeout=interval)

        logger.info("Notehub poll loop exiting")

    def _fetch_and_publish(self) -> None:
        """
        Single poll cycle:
          1. GET /v1/projects/{projectUID}/events-cursor
          2. Convert each event with coordinates to CoT XML
          3. Publish each CoT to RabbitMQ
          4. Persist the next_cursor for the next cycle
        """
        project_uid = self._config.get("OTS_NOTEHUB_PLUGIN_PROJECT_UID")
        api_key = self._config.get("OTS_NOTEHUB_PLUGIN_API_KEY")
        notefile_filter = self._config.get("OTS_NOTEHUB_PLUGIN_NOTEFILE_FILTER", "")
        cot_type = self._config.get("OTS_NOTEHUB_PLUGIN_COT_TYPE", "a-f-G-U-C")
        stale_seconds = int(self._config.get("OTS_NOTEHUB_PLUGIN_COT_STALE_TIME", 300))
        rabbit_host = self._config.get("OTS_RABBITMQ_SERVER_ADDRESS", "127.0.0.1")
        ttl = str(self._config.get("OTS_RABBITMQ_TTL", "86400000"))
        poll_interval = int(self._config.get("OTS_NOTEHUB_PLUGIN_POLL_INTERVAL", 30))

        if not project_uid or not api_key:
            logger.warning(
                "Notehub plugin: OTS_NOTEHUB_PLUGIN_PROJECT_UID and "
                "OTS_NOTEHUB_PLUGIN_API_KEY must both be set in config.yml"
            )
            return

        headers = {"Authorization": f"Bearer {api_key}"}
        url = f"{NOTEHUB_API_BASE}/projects/{project_uid}/events-cursor"

        params: dict = {
            "limit": 50,
            "sortOrder": "asc",
        }

        if self._cursor:
            # Resume from where we left off
            params["cursor"] = self._cursor
        else:
            # First run: start from (now - one poll interval) to catch very
            # recent events without replaying the entire event history.
            params["startDate"] = int(time.time()) - poll_interval

        if notefile_filter:
            # e.g. "track.qo,sensor.qo"
            params["files"] = notefile_filter

        response = requests.get(url, headers=headers, params=params, timeout=20)

        # On cursor-not-found (invalid/expired cursor), Notehub returns 200
        # with the normal result set — the docs say an invalid cursor is
        # silently ignored — so we don't need special handling here.
        # On auth failure or project-not-found, raise_for_status will throw.
        response.raise_for_status()

        data = response.json()
        events: list[dict] = data.get("events") or []
        next_cursor: str = data.get("next_cursor", "")

        ingested = 0
        for ev in events:
            cot_xml = _event_to_cot_xml(ev, cot_type, stale_seconds)
            if cot_xml:
                uid = ev.get("best_id") or ev.get("device") or "unknown"
                _publish_cot(uid, cot_xml, rabbit_host, ttl)
                ingested += 1

        if next_cursor:
            self._cursor = next_cursor
            self._save_cursor()

        if ingested:
            logger.info(
                "Notehub: published %d / %d events as CoT (cursor=%s)",
                ingested, len(events), self._cursor,
            )
        elif events:
            logger.debug(
                "Notehub: %d events received, none had usable coordinates",
                len(events),
            )

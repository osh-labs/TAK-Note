# TAK-Note

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)

**TAK-Note** is an [OpenTAKServer](https://opentakserver.io) plugin that bridges [Blues Wireless](https://blues.com) Notecard IoT hardware with TAK-family situational awareness tools. It ingests device telemetry from [Notehub.io](https://notehub.io) and publishes each event as a Cursor-on-Target (CoT) message, making Notecard-equipped assets visible on ATAK, WinTAK, and iTAK EUDs in real time.

Notecards are compact, low-power cellular and satellite modems designed for remote environments where power and connectivity are limited. This plugin is aimed at teams that already operate a TAK common operating picture and want to add Notecard-equipped personnel, vehicles, or sensors to it — without additional infrastructure or a SIM management contract.

Developed and maintained by [Chris Lee](https://github.com/osh-labs) at [OSH-Labs](https://github.com/osh-labs). Released as free software under the GNU General Public License v3.

---

## Architecture

```
Notecard hardware
  │  (cellular / satellite / LoRa / WiFi)
  ▼
Notehub.io
  │
  ├── POLLING PATH (default, no public-facing server required)
  │     OTS plugin polls every N seconds
  │     GET /v1/projects/{projectUID}/events-cursor
  │                    │
  └── WEBHOOK PATH (optional, requires public HTTPS on OTS host)
        Notehub HTTP Route POSTs to /api/notehub/webhook
                       │
                       ▼
           OTS-Notehub-Plugin (this plugin)
                       │
                       ▼  json.dumps({"uid": uid, "cot": xml})
           RabbitMQ — cot_controller exchange
                       │
                       ▼
           OTS cot_parser (parses, persists, routes)
                       │
                       ▼  RabbitMQ — cot fanout exchange
           ATAK / WinTAK / iTAK EUDs
```

---

## Prerequisites

- OpenTAKServer ≥ 1.5.0 (plugin system introduced in 1.5.0)
- A [Notehub.io](https://notehub.io) account and project
- One or more Notecard-equipped devices sending events to that project
- Python ≥ 3.11 (included in the OTS virtual environment)

---

## Installation

```bash
~/.opentakserver_venv/bin/pip install git+https://github.com/osh-labs/TAK-Note.git
```

After installation, restart OpenTAKServer.  The plugin will appear in the OTS web UI under Plugins.

---

## Notehub Personal Access Token

1. Log in to [notehub.io](https://notehub.io)
2. Click your avatar → **Account Settings** → **Access Tokens**
3. Create a new token; assign it the **viewer** role on your project
4. Copy the token — it is shown only once

---

## Configuration

Edit `~/ots/config.yml` and add the following block.  Only the first three keys are required.

```yaml
# --- Required ---
OTS_NOTEHUB_PLUGIN_ENABLED: true
OTS_NOTEHUB_PLUGIN_API_KEY: "v2:your-notehub-personal-access-token"
OTS_NOTEHUB_PLUGIN_PROJECT_UID: "app:2606f411-dea6-44a0-9743-1130f57d77d8"

# --- Optional (defaults shown) ---
OTS_NOTEHUB_PLUGIN_POLL_INTERVAL: 30          # seconds between API polls
OTS_NOTEHUB_PLUGIN_NOTEFILE_FILTER: ""        # e.g. "track.qo,sensor.qo" or leave blank for all
OTS_NOTEHUB_PLUGIN_COT_TYPE: "a-f-G-U-C"     # CoT type; controls ATAK icon
OTS_NOTEHUB_PLUGIN_COT_STALE_TIME: 300        # seconds before CoT point goes stale on EUDs
OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED: false     # enable push delivery via Notehub Route
OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET: ""         # shared secret for webhook auth
```

Restart OTS after editing config.yml:
```bash
sudo systemctl restart opentakserver
```

> **Note:** Only the `opentakserver` process loads plugins. The `cot_parser`, `eud_handler`, and `eud_handler_ssl` services do not need to restart for plugin configuration changes — they communicate with the plugin via RabbitMQ and do not import plugin code directly.

---

## CoT Type Reference

Select the type appropriate for your deployment:

| `OTS_NOTEHUB_PLUGIN_COT_TYPE` | ATAK icon                    |
|-------------------------------|------------------------------|
| `a-f-G-U-C`                   | Friendly ground unit         |
| `a-f-G-U-C-I`                 | Friendly infantry            |
| `a-f-G-E-V`                   | Friendly ground vehicle      |
| `a-u-G-U-C`                   | Unknown ground unit          |
| `a-n-G`                       | Neutral ground               |
| `b-m-p-s-p-loc`               | Generic sensor/marker        |

Full type schema: [TAK CoT Types](https://github.com/deptofdefense/mil-sym-java/blob/master/renderer/src/main/java/sec/web/proxy/SpotReport.java)

---

## Cursor Persistence

The plugin uses Notehub's cursor-based pagination to fetch only new events on each poll cycle.  The cursor is written to:

```
~/ots/notehub_cursor.txt
```

This file is read on startup so polling resumes exactly where it left off after an OTS restart.  Delete the file to force a full re-poll from the current time.

---

## CoT UID and Callsign Mapping

| Source field          | CoT field            | Fallback                     |
|-----------------------|----------------------|------------------------------|
| `event.device`        | `event[@uid]`        | random UUID                  |
| `event.sn`            | `contact[@callsign]` | `event.best_id`, then `device`|
| `event.best_lat/lon`  | `point[@lat/lon]`    | `where_*`, `tri_*`, `tower_*`|
| `event.when`          | `event[@time/start]` | `event.received`, then now   |

**CoT UID format:** `Notehub-dev-<DeviceUID-without-colons>`  
Example: `Notehub-dev-5c0272311928`

---

## Notecard Body Field Mapping

Fields in the Notecard `note.add` body are mapped to CoT elements as follows:

| Body field       | CoT element / attribute          |
|------------------|----------------------------------|
| `alt`, `altitude`| `<point hae="...">`              |
| `speed`          | `<track speed="...">`            |
| `course`, `heading` | `<track course="...">`        |
| All other fields | `<remarks>key=value; ...</remarks>` |

Battery voltage from Notehub session metadata (`event.voltage`) is converted to an approximate percentage using a linear 3.0 V – 4.2 V scale and reported in `<status battery="...">`.

---

## Location Accuracy and Circular Error

The plugin sets CoT `ce` (circular error, metres) based on the Notehub-reported location type:

| `best_location_type` | `ce` value  | Notes                              |
|----------------------|-------------|------------------------------------|
| `gps`                | 50          | Notecard GNSS typical accuracy     |
| `triangulated`       | 2000        | Cell-tower / WiFi triangulation    |
| `tower`              | 9999999     | Single cell tower centroid (unknown)|

ATAK uses `ce` to draw an accuracy circle around the dot.  Points with `ce=9999999` will show a very large accuracy circle.

---

## Webhook Setup (Optional)

Use this path when you need sub-30-second latency and your OTS instance has a public HTTPS address.

### 1. Enable the webhook in config.yml

```yaml
OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED: true
OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET: "replace-with-a-strong-random-string"
```

### 2. Configure a Notehub HTTP Route

1. In Notehub, navigate to your project → **Routes** → **Add Route**
2. Route type: **HTTP/HTTPS**
3. URL: `https://your-ots-host/api/notehub/webhook`
4. Add header: `X-Notehub-Secret` → `<your secret>`
5. Select which Notefiles to route (or route all)
6. Save and enable the route

Notehub will POST each event to the webhook endpoint as it arrives.  The plugin validates the secret, converts the event to CoT, and injects it immediately.

---

## Notecard Firmware Considerations

For best results on ATAK, configure your Notecard host firmware to:

### Send GPS location with each note

```c
// Enable continuous GPS tracking
J *req = NoteNewRequest("card.location.mode");
JAddStringToObject(req, "mode", "continuous");
NoteRequest(req);

// Send a note with GPS location every 60 seconds
req = NoteNewRequest("note.add");
JAddStringToObject(req, "file", "track.qo");
J *body = JAddObjectToObject(req, "body");
// plugin will use Notehub best_lat/best_lon
// optionally add altitude and speed:
JAddNumberToObject(body, "alt", gps_altitude_metres);
JAddNumberToObject(body, "speed", speed_m_per_s);
JAddNumberToObject(body, "course", heading_degrees);
NoteRequest(req);
```

### Set a meaningful serial number (callsign)

```c
J *req = NoteNewRequest("hub.set");
JAddStringToObject(req, "sn", "TEAM1-ALPHA");  // becomes ATAK callsign
NoteRequest(req);
```

### Recommended Notefile

`track.qo` is the conventional Notecard queue for location/tracking data.  Set `OTS_NOTEHUB_PLUGIN_NOTEFILE_FILTER: "track.qo"` to ingest only tracking notes and ignore session / health system files.

---

## Status Endpoint

```
GET /api/notehub/status
```

Returns a JSON object with non-sensitive plugin configuration and polling state.  No authentication required.

---

## Development / Build

```bash
git clone https://github.com/osh-labs/TAK-Note.git
cd TAK-Note

# Install Poetry dependencies (includes pytest, black, and dev extras)
poetry install --extras dev

# Run the test suite
poetry run pytest

# Install in development mode into the OTS venv
~/.opentakserver_venv/bin/pip install -e ".[dev]"

# Build a distributable wheel
poetry build
```

---

## Contributing

Contributions are welcome. To get started:

1. Fork the repository and clone your fork
2. Install development dependencies: `poetry install --extras dev`
3. Make your changes on a feature branch
4. Ensure the test suite passes: `poetry run pytest`
5. Open a pull request against `main` with a clear description of the change

Please open an issue before starting significant work so we can discuss the approach. All contributions must be compatible with the GPL-3.0 license.

---

## License

Copyright (C) 2024 Chris Lee / OSH-Labs

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0.html) for more details.

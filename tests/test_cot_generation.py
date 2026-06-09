"""
Tests for TAK-Note CoT generation and location resolution.

Run with:
    pytest tests/test_cot_generation.py -v

These tests do NOT require an OTS or RabbitMQ instance.
"""

import json
from xml.etree.ElementTree import fromstring

import pytest

from tak_note.app import (
    _event_to_cot_xml,
    _resolve_location,
)

# ---------------------------------------------------------------------------
# Sample Notehub events
# ---------------------------------------------------------------------------

GPS_EVENT = {
    "event": "dfa3747d-688b-4250-935b-5dd60354313c",
    "device": "dev:5c0272311928",
    "sn": "TEAM1-ALPHA",
    "best_id": "TEAM1-ALPHA",
    "received": 1656011227.0,
    "when": 1656010061,
    "file": "track.qo",
    "body": {
        "alt": 350.0,
        "speed": 5.2,
        "course": 270.0,
        "temperature": 24.0,
    },
    "best_location_type": "gps",
    "best_lat": 34.82476372,
    "best_lon": -83.32261614,
    "where_lat": 34.82476372,
    "where_lon": -83.32261614,
    "voltage": 3.7,
}

TRIANGULATED_EVENT = {
    "event": "abc123",
    "device": "dev:aabbcc112233",
    "sn": "SENSOR-2",
    "best_location_type": "triangulated",
    "best_lat": 34.5,
    "best_lon": -83.0,
    "tri_lat": 34.5,
    "tri_lon": -83.0,
    "body": {"humidity": 65.0},
    "when": 1656010000,
    "voltage": 4.0,
}

TOWER_ONLY_EVENT = {
    "event": "xyz789",
    "device": "dev:001122334455",
    "best_id": "SENSOR-3",
    "best_location_type": "tower",
    "best_lat": 33.9,
    "best_lon": -84.1,
    "tower_lat": 33.9,
    "tower_lon": -84.1,
    "body": {},
    "when": 1656010100,
}

NO_LOCATION_EVENT = {
    "event": "noloc",
    "device": "dev:deadbeef",
    "body": {"temp": 25.0},
    "when": 1656010200,
}


# ---------------------------------------------------------------------------
# _resolve_location tests
# ---------------------------------------------------------------------------

class TestResolveLocation:
    def test_gps_event(self):
        lat, lon, ce, loc_type = _resolve_location(GPS_EVENT)
        assert lat == pytest.approx(34.82476372)
        assert lon == pytest.approx(-83.32261614)
        assert ce == "50"
        assert loc_type == "gps"

    def test_triangulated_event(self):
        lat, lon, ce, loc_type = _resolve_location(TRIANGULATED_EVENT)
        assert lat == pytest.approx(34.5)
        assert ce == "2000"
        assert loc_type == "triangulated"

    def test_tower_event(self):
        lat, lon, ce, loc_type = _resolve_location(TOWER_ONLY_EVENT)
        assert lat == pytest.approx(33.9)
        assert ce == "9999999"
        assert loc_type == "tower"

    def test_no_location_returns_none(self):
        lat, lon, ce, loc_type = _resolve_location(NO_LOCATION_EVENT)
        assert lat is None
        assert lon is None

    def test_fallback_to_best_when_where_absent(self):
        ev = {
            "best_location_type": "triangulated",
            "best_lat": 35.0,
            "best_lon": -80.0,
        }
        lat, lon, ce, _ = _resolve_location(ev)
        assert lat == pytest.approx(35.0)


# ---------------------------------------------------------------------------
# _event_to_cot_xml tests
# ---------------------------------------------------------------------------

COT_TYPE = "a-f-G-U-C"
STALE_SECONDS = 300


class TestEventToCotXml:
    def _parse(self, event):
        xml_str = _event_to_cot_xml(event, COT_TYPE, STALE_SECONDS)
        assert xml_str is not None
        return fromstring(xml_str)

    def test_returns_none_for_no_location(self):
        result = _event_to_cot_xml(NO_LOCATION_EVENT, COT_TYPE, STALE_SECONDS)
        assert result is None

    def test_uid_format(self):
        root = self._parse(GPS_EVENT)
        assert root.attrib["uid"] == "Notehub-dev-5c0272311928"

    def test_uid_strips_colon(self):
        root = self._parse(GPS_EVENT)
        assert ":" not in root.attrib["uid"]

    def test_cot_type(self):
        root = self._parse(GPS_EVENT)
        assert root.attrib["type"] == COT_TYPE

    def test_how_is_machine_generated(self):
        root = self._parse(GPS_EVENT)
        assert root.attrib["how"] == "m-g"

    def test_point_coordinates(self):
        root = self._parse(GPS_EVENT)
        point = root.find("point")
        assert float(point.attrib["lat"]) == pytest.approx(34.82476372)
        assert float(point.attrib["lon"]) == pytest.approx(-83.32261614)

    def test_gps_ce_value(self):
        root = self._parse(GPS_EVENT)
        point = root.find("point")
        assert point.attrib["ce"] == "50"

    def test_triangulated_ce_value(self):
        root = self._parse(TRIANGULATED_EVENT)
        point = root.find("point")
        assert point.attrib["ce"] == "2000"

    def test_tower_ce_value(self):
        root = self._parse(TOWER_ONLY_EVENT)
        point = root.find("point")
        assert point.attrib["ce"] == "9999999"

    def test_hae_from_body_alt(self):
        root = self._parse(GPS_EVENT)
        point = root.find("point")
        assert float(point.attrib["hae"]) == pytest.approx(350.0)

    def test_callsign_from_sn(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        contact = detail.find("contact")
        assert contact.attrib["callsign"] == "TEAM1-ALPHA"

    def test_callsign_fallback_to_best_id(self):
        ev = dict(TOWER_ONLY_EVENT)  # has best_id, no sn
        root = self._parse(ev)
        detail = root.find("detail")
        contact = detail.find("contact")
        assert contact.attrib["callsign"] == "SENSOR-3"

    def test_track_element_when_speed_and_course_present(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        track = detail.find("track")
        assert track is not None
        assert float(track.attrib["speed"]) == pytest.approx(5.2)
        assert float(track.attrib["course"]) == pytest.approx(270.0)

    def test_track_element_absent_when_not_in_body(self):
        root = self._parse(TOWER_ONLY_EVENT)
        detail = root.find("detail")
        assert detail.find("track") is None

    def test_battery_status_present(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        status = detail.find("status")
        assert status is not None
        pct = int(status.attrib["battery"])
        # 3.7 V on 3.0–4.2 V scale ≈ 58%
        assert 50 <= pct <= 70

    def test_battery_status_absent_when_no_voltage(self):
        root = self._parse(TOWER_ONLY_EVENT)
        detail = root.find("detail")
        assert detail.find("status") is None

    def test_remarks_contain_extra_body_fields(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        remarks = detail.find("remarks")
        assert remarks is not None
        assert "temperature" in remarks.text

    def test_remarks_exclude_mapped_fields(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        remarks = detail.find("remarks")
        # alt, speed, course should NOT appear in remarks
        for key in ("alt", "speed", "course"):
            assert key not in (remarks.text or "")

    def test_flow_tag_present(self):
        root = self._parse(GPS_EVENT)
        detail = root.find("detail")
        flow = detail.find("_flow-tags_")
        assert flow is not None
        assert "TAK-Note" in flow.attrib

    def test_stale_is_after_start(self):
        from datetime import datetime, timezone
        root = self._parse(GPS_EVENT)
        start_str = root.attrib["start"]
        stale_str = root.attrib["stale"]
        fmt = "%Y-%m-%dT%H:%M:%S.000Z"
        start_dt = datetime.strptime(start_str, fmt).replace(tzinfo=timezone.utc)
        stale_dt = datetime.strptime(stale_str, fmt).replace(tzinfo=timezone.utc)
        delta = (stale_dt - start_dt).total_seconds()
        assert delta == pytest.approx(STALE_SECONDS, abs=1)

    def test_xml_is_well_formed(self):
        """fromstring raises ParseError for malformed XML; this validates structure."""
        for ev in (GPS_EVENT, TRIANGULATED_EVENT, TOWER_ONLY_EVENT):
            xml_str = _event_to_cot_xml(ev, COT_TYPE, STALE_SECONDS)
            root = fromstring(xml_str)
            assert root.tag == "event"

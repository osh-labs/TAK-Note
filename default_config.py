"""
Default configuration for OTS-Notehub-Plugin.

All keys prefixed with OTS_NOTEHUB_PLUGIN_ are read from ~/ots/config.yml.
Add only the keys you need to override; absent keys use the values below.

Example minimal config.yml addition:
    OTS_NOTEHUB_PLUGIN_ENABLED: true
    OTS_NOTEHUB_PLUGIN_API_KEY: "v2:abc123..."
    OTS_NOTEHUB_PLUGIN_PROJECT_UID: "app:2606f411-dea6-44a0-9743-1130f57d77d8"
"""


class Config:
    # --- Required when plugin is enabled ---

    # Enable the plugin.  Set to true to start polling.
    OTS_NOTEHUB_PLUGIN_ENABLED: bool = False

    # Notehub Personal Access Token.
    # Create at: notehub.io → Account → Access Tokens
    # Minimum required role: viewer (project-level)
    OTS_NOTEHUB_PLUGIN_API_KEY: str = ""

    # Notehub ProjectUID or ProductUID.
    # Format: "app:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    # Found at: notehub.io → your project → Settings → Project UID
    OTS_NOTEHUB_PLUGIN_PROJECT_UID: str = ""

    # --- Polling ---

    # Interval between Notehub API polls, in seconds.
    # Lower values reduce latency but increase API call volume.
    # Notehub Essentials retains events for 7 days; polling at 30s is safe.
    OTS_NOTEHUB_PLUGIN_POLL_INTERVAL: int = 30

    # Comma-separated list of Notefiles to ingest.
    # Leave empty to ingest all non-system Notefiles.
    # Example: "track.qo,sensor.qo"
    # Common Notecard system Notefiles: _session.qo, _health.qo, _track.qo
    OTS_NOTEHUB_PLUGIN_NOTEFILE_FILTER: str = ""

    # --- CoT generation ---

    # CoT type attribute.  Controls the icon displayed on ATAK.
    # Common values:
    #   a-f-G-U-C   Friendly Ground Unit (Combat)    [default]
    #   a-f-G-U-C-I Friendly Ground Unit (Infantry)
    #   a-u-G-U-C   Unknown Ground Unit
    #   a-n-G       Neutral Ground
    # Reference: MIL-STD-2525C / CoT type schema
    OTS_NOTEHUB_PLUGIN_COT_TYPE: str = "a-f-G-U-C"

    # How long (seconds) before a CoT point is considered stale on EUDs.
    # Set to at least 2× your poll interval to avoid points blinking out
    # between updates.  Default 300 s (5 minutes).
    OTS_NOTEHUB_PLUGIN_COT_STALE_TIME: int = 300

    # --- Webhook (optional, for Notehub HTTP Route push delivery) ---

    # Enable the /api/notehub/webhook POST endpoint.
    # Only useful when the OTS server has a publicly reachable HTTPS address.
    # When disabled the endpoint returns 404.
    OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED: bool = False

    # Shared secret used to authenticate incoming Notehub webhook requests.
    # If set, each POST must include the header:
    #     X-Notehub-Secret: <this value>
    # Leave empty to skip secret validation (not recommended for production).
    OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET: str = ""

    @staticmethod
    def validate(config: dict) -> list[str]:
        """
        Called by the plugin framework to validate user-supplied config.
        Returns a list of error strings (empty list = valid).
        """
        errors: list[str] = []

        if not config.get("OTS_NOTEHUB_PLUGIN_ENABLED", False):
            # Plugin disabled — nothing more to validate
            return errors

        if not config.get("OTS_NOTEHUB_PLUGIN_API_KEY", "").strip():
            errors.append(
                "OTS_NOTEHUB_PLUGIN_API_KEY is required when "
                "OTS_NOTEHUB_PLUGIN_ENABLED is true"
            )

        project_uid = config.get("OTS_NOTEHUB_PLUGIN_PROJECT_UID", "").strip()
        if not project_uid:
            errors.append(
                "OTS_NOTEHUB_PLUGIN_PROJECT_UID is required when "
                "OTS_NOTEHUB_PLUGIN_ENABLED is true"
            )
        elif not (project_uid.startswith("app:") or project_uid.startswith("product:")):
            errors.append(
                "OTS_NOTEHUB_PLUGIN_PROJECT_UID must start with 'app:' or 'product:'"
            )

        poll_interval = config.get("OTS_NOTEHUB_PLUGIN_POLL_INTERVAL", 30)
        try:
            if int(poll_interval) < 10:
                errors.append(
                    "OTS_NOTEHUB_PLUGIN_POLL_INTERVAL must be at least 10 seconds"
                )
        except (ValueError, TypeError):
            errors.append(
                "OTS_NOTEHUB_PLUGIN_POLL_INTERVAL must be an integer"
            )

        stale = config.get("OTS_NOTEHUB_PLUGIN_COT_STALE_TIME", 300)
        try:
            if int(stale) < 1:
                errors.append(
                    "OTS_NOTEHUB_PLUGIN_COT_STALE_TIME must be >= 1 second"
                )
        except (ValueError, TypeError):
            errors.append(
                "OTS_NOTEHUB_PLUGIN_COT_STALE_TIME must be an integer"
            )

        if config.get("OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED", False):
            if not config.get("OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET", "").strip():
                errors.append(
                    "OTS_NOTEHUB_PLUGIN_WEBHOOK_SECRET should be set when "
                    "OTS_NOTEHUB_PLUGIN_WEBHOOK_ENABLED is true (strongly recommended)"
                )

        return errors

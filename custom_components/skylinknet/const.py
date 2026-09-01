DOMAIN = "skylinknet"

API_URL = "https://api-1.skyhm.net"

LOGIN_ENDPOINT = "/guest/login"
GET_HUB_ENDPOINT = "/api/user/get_hub"
GET_STATUS_ENDPOINT = "/api/hub/get_status"
GET_DEV_ENDPOINT = "/api/dev/get_dev"
READ_ENDPOINT = "/api/dev/read"
SET_ALARM_ENDPOINT = "/api/alarm/set_alarm"

# WebSocket shares the same host as API_URL, just over wss:// instead
# of https://, so it is derived from it rather than duplicated.
WEBSOCKET_ENDPOINT = "/websock/hu/{hub_id}/{hub_key}"

# Timeout (seconds) applied to every SkylinkNet HTTP request, so a
# hung server can't block the integration indefinitely.
REQUEST_TIMEOUT = 20

# ============================================================
# DEVICE TYPES (SkylinkNet "dev_type" field)
# ============================================================

DEV_TYPE_DOOR = 4
DEV_TYPE_MOTION = 6
DEV_TYPE_WINDOW = 11

# ============================================================
# ALARM STATE CODES
#
# Valorile provin din aplicația SkylinkNet:
# 2 = home, 3 = away, 4 = disarm
# ============================================================

ALARM_CODE_ARMED_HOME = 2
ALARM_CODE_ARMED_AWAY = 3
ALARM_CODE_DISARMED = 4

# ============================================================
# PERSISTENT STORAGE
#
# Folosit pentru a păstra lista de dispozitive cunoscute, cele
# ignorate ("uitate") și ultima stare cunoscută a fiecăruia,
# astfel încât senzorii să nu rămână "unavailable" la un
# restart până sosește primul mesaj WebSocket.
# ============================================================

STORAGE_VERSION = 1

# ============================================================
# SERVICES
# ============================================================

SERVICE_FORGET_DEVICE = "forget_device"
SERVICE_ALLOW_DEVICE = "allow_device"

CONF_SKYLINKNET_DEVICE_ID = "skylinknet_device_id"
CONF_CONFIG_ENTRY_ID = "config_entry_id"
CONF_IGNORE_FUTURE_EVENTS = "ignore_future_events"

ATTR_DEVICE_ID = "device_id"

# ============================================================
# DEFAULT DEVICE CLASS
#
# Folosită doar pentru dispozitive descoperite dinamic prin
# WebSocket, pentru care nu avem un "dev_type" cunoscut (deci
# nu se pot încadra automat la door/window/motion).
# ============================================================

CONF_DEFAULT_DEVICE_CLASS = "default_device_class"
DEFAULT_DEVICE_CLASS = "motion"
DEVICE_CLASS_NONE = "none"

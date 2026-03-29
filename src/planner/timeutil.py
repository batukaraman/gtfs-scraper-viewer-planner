"""GTFS HH:MM:SS to seconds (service day; hours may exceed 24)."""


def gtfs_time_to_seconds(t: str) -> int:
    parts = str(t).strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def seconds_to_gtfs_time(sec: int) -> str:
    sec = max(0, sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

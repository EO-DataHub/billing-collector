from datetime import datetime


def bytes_avg_to_gb_seconds(avg_bytes: float, interval_sec: int) -> float:
    return (avg_bytes * interval_sec) / 1024**3


def parse_iso_timestamp(iso_time: str) -> datetime:
    """
    Parse an ISO8601 timestamp string into a datetime object.
    """
    try:
        return datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"Invalid ISO8601 timestamp: {iso_time}") from e


def parse_workspace_name(workspace: str) -> str:
    """
    Parse the workspace name to extract the actual name.
    """
    if workspace.startswith("ws-"):
        return workspace[3:]
    else:
        return workspace

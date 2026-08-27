import sqlite3
from datetime import datetime

def update_heartbeat(db_connection, asset_id):
    epoch_ms = datetime.now().timestamp() * 1000
    db_connection.execute(
        "UPDATE heartbeats SET last_seen = ? WHERE asset_id = ?",
        (int(epoch_ms), asset_id)
    )
    db_connection.commit()
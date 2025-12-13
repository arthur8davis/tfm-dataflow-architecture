import json
import logging

def parse_json(json_str):
    try:
        return json.loads(json_str)
    except Exception as e:
        # logging.error(f"Error parsing JSON: {e}")
        return None

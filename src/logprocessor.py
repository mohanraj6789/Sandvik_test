import os
import re
import json
import asyncio
import aiofiles
from collections import defaultdict, Counter
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# Define log regex pattern
LOG_PATTERN = re.compile(r"\[(.*?)\] (.*?) (INFO|ERROR|WARN) (.*)")

# Directory containing log files
LOG_DIR = "logs"
OUTPUT_FILE = "output_processed_logs.json"

# Ensure the logs directory exists
os.makedirs(LOG_DIR, exist_ok=True)


def parse_log_line(line):
    """Parse a single log line into a structured dictionary."""
    match = LOG_PATTERN.match(line)
    if match:
        timestamp, server, level, message = match.groups()
        return {"timestamp": timestamp, "server": server, "level": level, "message": message}
    return None


async def process_log_file(filename, aggregated_data):
    """Process a single log file and update aggregated data."""
    try:
        async with aiofiles.open(filename, mode='r') as file:
            async for line in file:
                log_entry = parse_log_line(line.strip())
                if log_entry:
                    '''aggregated_data["logs"].append(log_entry)'''
                    aggregated_data["log_counts"][log_entry["level"]] += 1

                    # Track user activity
                    if "User '" in log_entry["message"] and "logged" or "accessed" in log_entry["message"]:
                        user = re.search(r"User '(.*?)'", log_entry["message"]).group(1)
                        if user not in aggregated_data["user_activity"]:
                            aggregated_data["user_activity"][user] = {"last_seen": log_entry["timestamp"], "actions": 0}
                        aggregated_data["user_activity"][user]["last_seen"] = log_entry["timestamp"]
                        aggregated_data["user_activity"][user]["actions"] += 1

                    # Count API errors
                    if "API request failed" in log_entry["message"]:
                        aggregated_data["api_errors"]["failed_requests"] += 1
                    aggregated_data["api_errors"]["total_requests"] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")


def calculate_error_rate(api_errors):
    """Calculate the API error rate."""
    if api_errors["total_requests"] > 0:
        error_rate = (api_errors["failed_requests"] / api_errors["total_requests"]) * 100
        api_errors["error_rate"] = f"{error_rate:.1f}%"
    else:
        api_errors["error_rate"] = "0%"


async def aggregate_logs():
    """Aggregate logs from all files in the log directory."""
    aggregated_data = {
        "log_counts": Counter(),
        "user_activity": defaultdict(lambda: {"last_seen": "", "actions": 0}),
        "api_errors": {"total_requests": 0, "failed_requests": 0, "error_rate": "0%"}
    }

    log_files = [os.path.join(LOG_DIR, f) for f in os.listdir(LOG_DIR) if f.endswith(".log")]
    await asyncio.gather(*(process_log_file(file, aggregated_data) for file in log_files))

    # Compute API error rate
    calculate_error_rate(aggregated_data["api_errors"])

    # Save to JSON
    async with aiofiles.open(OUTPUT_FILE, mode='w') as f:
        await f.write(json.dumps(aggregated_data, indent=4))


# FastAPI server to expose processed logs
app = FastAPI()


@app.get("/logs")
async def get_logs():
    """Endpoint to retrieve processed logs."""
    if not os.path.exists(OUTPUT_FILE):
        raise HTTPException(status_code=404, detail="Processed logs not found")
    async with aiofiles.open(OUTPUT_FILE, mode='r') as f:
        data = await f.read()
        return JSONResponse(content=json.loads(data))


# Run aggregation task
if __name__ == "__main__":
    asyncio.run(aggregate_logs())

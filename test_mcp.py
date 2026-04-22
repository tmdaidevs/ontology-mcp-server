"""End-to-end MCP protocol test."""
import subprocess
import sys
import time
import json
import threading

proc = subprocess.Popen(
    [sys.executable, "-B", "-m", "src.server"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

output_lines = []

def reader():
    for line in proc.stdout:
        output_lines.append(line.strip())

t = threading.Thread(target=reader, daemon=True)
t.start()

def send(msg):
    line = json.dumps(msg)
    proc.stdin.write(line + "\n")
    proc.stdin.flush()

def wait_for_response(timeout=15):
    start = time.time()
    initial_count = len(output_lines)
    while time.time() - start < timeout:
        if len(output_lines) > initial_count:
            return output_lines[-1]
        time.sleep(0.1)
    return None

# Initialize
send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": "test", "version": "1.0"}
}})
resp = wait_for_response()
print("INIT:", "OK" if resp else "NO RESPONSE")

# Initialized notification
send({"jsonrpc": "2.0", "method": "notifications/initialized"})
time.sleep(0.5)

# Call list_workspaces
print("Calling list_workspaces...")
send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
    "name": "list_workspaces",
    "arguments": {}
}})
resp = wait_for_response(timeout=20)
if resp:
    data = json.loads(resp)
    content = data.get("result", {}).get("content", [])
    structured = data.get("result", {}).get("structuredContent", None)
    if structured:
        workspaces = structured.get("result", [])
        print(f"\nSUCCESS! Found {len(workspaces)} workspaces:")
        for w in workspaces[:5]:
            print(f"  - {w.get('displayName')} ({w.get('id')})")
    elif content:
        text = content[0].get("text", "")
        try:
            workspaces = json.loads(text)
            print(f"\nSUCCESS! Found {len(workspaces)} workspaces:")
            for w in workspaces[:5]:
                print(f"  - {w.get('displayName')} ({w.get('id')})")
        except:
            print("TEXT:", text[:500])
    else:
        print("RESULT:", json.dumps(data, indent=2)[:500])
else:
    print("NO RESPONSE from tools/call (timed out after 20s)")
    proc.stderr.flush()
    err = proc.stderr.read() if proc.stderr.readable() else ""
    if err:
        print("STDERR:", err[:1000])

proc.kill()

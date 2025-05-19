import os

node_ids = ["1", "2", "3"]

targets = []
for node_id in node_ids:
    ip = os.environ.get(f"DIST_CONF_{node_id}_NODE_IP")
    port = os.environ.get(f"DIST_CONF_{node_id}_port", "2500")
    if ip:
        targets.append(f"'{ip}:{port}'")

with open("/etc/prometheus/prometheus.yml", "w") as f:
    f.write("global:\n  scrape_interval: 15s\n\n")
    f.write("scrape_configs:\n  - job_name: 'waverless'\n    static_configs:\n      - targets: [\n")
    for i, t in enumerate(targets):
        if i < len(targets) - 1:
            f.write(f"          {t},\n")
        else:
            f.write(f"          {t}\n")
    f.write("      ]\n") 
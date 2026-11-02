import json
import os
import subprocess
from pathlib import Path


def test_app_and_migrate_use_the_same_built_image() -> None:
    project_root = Path(__file__).resolve().parents[2]
    environment = {
        **os.environ,
        "POSTGRES_PASSWORD": "compose-config-test",
        "EMODA_ENV_FILE": ".env.example",
    }
    completed = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=project_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    services = json.loads(completed.stdout)["services"]

    assert services["app"]["image"] == services["migrate"]["image"]

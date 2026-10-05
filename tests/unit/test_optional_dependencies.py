import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


class OptionalDependencyTests(unittest.TestCase):
    def test_importing_core_does_not_load_cloud_framework_or_provider_sdks(self):
        project_root = Path(__file__).resolve().parents[2]
        source_root = project_root / "src"
        environment = os.environ.copy()
        existing_path = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = os.pathsep.join(
            path for path in (str(source_root), existing_path) if path
        )
        script = (
            "import importlib, json, sys; import tracelet; "
            "optional = {'boto3', 'sqlalchemy', 'httpx', 'fastapi', 'openai', 'anthropic'}; "
            "print(json.dumps(sorted(optional.intersection(sys.modules))))"
        )

        process = subprocess.run(
            [sys.executable, "-c", script],
            cwd=project_root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout), [])


if __name__ == "__main__":
    unittest.main()

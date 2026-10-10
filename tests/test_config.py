#!/usr/bin/env python3
"""Offline configuration/transport guards; no hosts, Docker, GPU or model calls."""
import contextlib
import ast
import copy
import io
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import common
import cluster

EXAMPLE = ROOT / "config.example.json"


def config(data=None):
    if data is None:
        return common.load_config(EXAMPLE)
    with patch.object(Path, "read_text", return_value=json.dumps(data)):
        return common.load_config(EXAMPLE)


class Receipt:
    def __init__(self, value):
        self.value = value

    def read_text(self):
        return json.dumps(self.value)


class ConfigTests(unittest.TestCase):
    def reject(self, change):
        data = json.loads(EXAMPLE.read_text())
        change(data)
        with self.assertRaises((ValueError, KeyError, TypeError)):
            config(data)

    def test_example_is_a_four_node_two_rail_cycle(self):
        cfg = config()
        self.assertEqual(len(cfg["nodes"]), 4)
        self.assertEqual(cfg["profile"]["parallel"], 8)
        self.assertEqual(len({f["address"].rsplit(".", 1)[0] for n in cfg["nodes"] for f in n["fabric"]}), 8)

    def test_reject_management_interface_reuse(self):
        self.reject(lambda c: c["nodes"][0]["fabric"][0].update(interface=c["nodes"][0]["management_interface"]))

    def test_reject_nonreciprocal_links(self):
        self.reject(lambda c: c["nodes"][0]["fabric"][0].update(peer_interface="wrong-netdev"))

    def test_reject_rail_subnet_collision(self):
        def collide(c):
            c["nodes"][0]["fabric"][1]["address"] = "10.77.41.5"
            c["nodes"][3]["fabric"][3]["address"] = "10.77.41.6"
        self.reject(collide)

    def test_reject_network_broadcast_and_duplicate_addresses(self):
        for address in ("10.77.41.0", "10.77.41.255", "10.77.141.1"):
            with self.subTest(address=address):
                self.reject(lambda c: c["nodes"][0]["fabric"][0].update(address=address))

    def test_reject_missing_rail_and_unsafe_state_component(self):
        self.reject(lambda c: c["nodes"][0]["fabric"].pop())
        self.reject(lambda c: c["nodes"][0].update(name=".."))

    def test_reject_nonfinite_cache_and_wrong_image(self):
        self.reject(lambda c: c["profile"].update(cache_gib=float("nan")))
        self.reject(lambda c: c["profile"].update(reserve_gib=float("inf")))
        self.reject(lambda c: c.update(image="moving-image:latest"))

    def test_reject_shared_download_and_compile_paths(self):
        for name in ("model_root", "cache_root"):
            with self.subTest(name=name):
                self.reject(lambda c: c.update({name: c["shared_root"] + "/local-locks"}))
        cfg = config()
        cfg["model_dir"] = cfg["shared_root"] + "/models"
        with self.assertRaises(ValueError):
            common.validate_local_storage(cfg)

    def test_downloader_refuses_network_storage_before_writes_or_downloads(self):
        spec = importlib.util.spec_from_file_location("portable_downloader", ROOT / "scripts/download-models.py")
        downloader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(downloader)
        with patch.object(sys, "argv", ["download-models.py", "--config", str(EXAMPLE)]), \
                patch.object(downloader.subprocess, "check_output", return_value="nfs\n"), \
                patch.object(Path, "mkdir", side_effect=AssertionError("unexpected download directory")), \
                patch.object(downloader.urllib.request, "urlopen", side_effect=AssertionError("unexpected download")), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                downloader.main()

    def test_auto_window_matches_mia_all_five_to_eight_lanes(self):
        for lanes in range(1, 9):
            p = common.validate_profile({"parallel": lanes})
            self.assertEqual(p["multi_window"], 64 if lanes > 4 else 32)

    def test_environment_and_commands_are_config_driven(self):
        data = json.loads(EXAMPLE.read_text())
        data["nodes"][2]["management_interface"] = "mgmt3"
        data["nodes"][2]["fabric"][0]["hca"] = "custom-hca"
        cfg = config(data)
        cfg["model_dir"] = cfg["model_root"] + "/glm53-exl3"
        cfg["drafter_dir"] = cfg["model_root"] + "/dflash2"
        op = cluster.Operator(cfg)
        for rank, node in enumerate(cfg["nodes"]):
            env = common.environment(cfg, rank, "a" * 64)
            self.assertEqual(env["NCCL_IB_HCA"], "=" + ",".join(f["hca"] + ":1" for f in node["fabric"]))
            self.assertEqual(env["NCCL_SOCKET_IFNAME"], "=" + node["management_interface"])
            for key, value in {"TF_GLM_COMM": "nccl", "TF_GLM_HC_EXCHANGE": "gather", "TF_GLM_DENSE": "q4",
                               "TF_GLM_KV": "fp8", "TF_GLM_DFLASH_POLICY": "fnc7:0.3", "TF_GLM_HC_SPLIT": "1",
                               "TF_GLM_PREFILL_OVERLAP": "2", "TF_GLM_KDA_CHUNKED": "1", "TF_GLM_COPY_MAX": "15",
                               "TF_GLM_WIDE_GRAPHS": "16", "TF_GLM_L2PF": "1", "TF_GLM_EXL3_LOADS": "nc",
                               "TENSORFOLD_NUCLEUS_UNION": "1", "TF_GLM_MULTI_PREFILL": "1",
                               "TF_GLM_CACHE_ENTRIES": "32", "TF_GLM_MULTI_WINDOW": "64"}.items():
                self.assertEqual(env[key], value)
            cache, command = op.command(rank, common.library_path(cfg), "a" * 64)
            self.assertEqual(command[command.index("--rank") + 1], str(rank))
            self.assertEqual(command[command.index("--tp") + 1], "4")
            self.assertIn(common.IMAGE, command)
            self.assertIn("/opt/spark-ring/scripts/runtime.py", command)
            self.assertIn(cfg["shared_root"] + "/scripts", " ".join(command))
            self.assertTrue(str(cache).startswith(cfg["cache_root"] + "/"))

    def test_qualification_requires_exact_identity_and_all_functions(self):
        cfg, digest = config(), "a" * 64
        doc = {flag: True for flag in ("qualified", "correctness_passed", "payload_all_hcas_passed", "cuda_graph_passed",
                                       "host_configuration_unchanged", "qp_diagnostics_all_hcas_passed")}
        doc.update(library_sha256=digest, image_digest=common.IMAGE,
                   hosts=[n["name"] for n in cfg["nodes"]], topology_sha256=common.topology_sha256(cfg),
                   required_hcas_by_host={n["name"]: [f["hca"] for f in n["fabric"]] for n in cfg["nodes"]},
                   transport_env_by_host={n["name"]: common.transport_environment(common.environment(cfg, rank, digest))
                                          for rank, n in enumerate(cfg["nodes"])})
        op = cluster.Operator(cfg)
        self.assertEqual(op.qualification(Receipt(doc), digest), doc)
        for mutation in (
                lambda d: d.update(library_sha256="b" * 64),
                lambda d: d.update(image_digest="other-image"),
                lambda d: d.update(cuda_graph_passed=False),
                lambda d: d.update(host_configuration_unchanged=False),
                lambda d: d.update(qp_diagnostics_all_hcas_passed=False),
                lambda d: d.update(topology_sha256="changed-topology"),
                lambda d: d["required_hcas_by_host"]["spark2"].pop(),
                lambda d: d["transport_env_by_host"]["spark3"].update(NCCL_MAX_NCHANNELS="1")):
            changed = copy.deepcopy(doc)
            mutation(changed)
            with self.assertRaises(RuntimeError):
                op.qualification(Receipt(changed), digest)

    def test_strict_ssh_and_no_implicit_remote_action(self):
        cfg = config()
        for node in cfg["nodes"]:
            argv = common.ssh_args(node, cfg)
            self.assertEqual(argv[-1], node["ssh"])
            for option in ("BatchMode=yes", "IdentitiesOnly=yes", "IdentityAgent=none", "UpdateHostKeys=no",
                           "StrictHostKeyChecking=yes", "HostKeyAlias=" + node["host_key_alias"]):
                self.assertIn(option, argv)
        with patch.object(sys, "argv", ["cluster.py", "status"]), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cluster.main()

    def test_remote_root_shell_preserves_literal_script(self):
        cfg = config()
        result = type("Result", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()
        with patch.object(cluster.subprocess, "run", return_value=result) as run:
            cluster.Operator(cfg).remote(0, "printf '%s' '$literal'", timeout=12)
        argv = run.call_args.args[0]
        self.assertEqual(argv[-2], cfg["nodes"][0]["ssh"])
        self.assertEqual(argv[-1], "sudo -n -s <<'SPARK_RING_OPERATOR_SCRIPT'\nset -eu\nprintf '%s' '$literal'\nSPARK_RING_OPERATOR_SCRIPT")

    def test_generated_preflight_is_valid_python_without_executing_it(self):
        cfg = config()
        cfg["model_dir"] = cfg["model_root"] + "/glm53-exl3"
        cfg["drafter_dir"] = cfg["model_root"] + "/dflash2"
        def inspect_code(rank, code):
            ast.parse(code)
            return "{}"
        op = cluster.Operator(cfg)
        with patch.object(op, "remote_python", side_effect=inspect_code):
            self.assertEqual(op.preflight(0, common.library_path(cfg), "a" * 64), {})

    def test_preflight_detects_same_size_small_file_corruption(self):
        cfg = config()
        cfg["model_dir"] = cfg["model_root"] + "/glm53-exl3"
        cfg["drafter_dir"] = cfg["model_root"] + "/dflash2"
        captured = []
        def capture(rank, code):
            captured.append(ast.parse(code))
            return "{}"
        op = cluster.Operator(cfg)
        with patch.object(op, "remote_python", side_effect=capture):
            op.preflight(0, common.library_path(cfg), "a" * 64)
        # Execute the actual generated receipt loop, without its remote/GPU shell.
        loop = next(node for node in ast.walk(captured[0]) if isinstance(node, ast.For)
                    and isinstance(node.target, ast.Name) and node.target.id == "file")
        code = compile(ast.Module(body=[loop], type_ignores=[]), "preflight-small-file-check", "exec")
        with tempfile.TemporaryDirectory(prefix="offline-small-file-", dir=ROOT) as temporary:
            directory = Path(temporary)
            file = directory / "tokenizer.json"
            file.write_bytes(b"aaaa")
            scope = {"directory": directory, "pathlib": __import__("pathlib"), "hashlib": hashlib,
                     "receipt": {"files": [{"path": file.name, "size": 4,
                                            "sha256": hashlib.sha256(b"aaaa").hexdigest()}]}}
            exec(code, scope)
            file.write_bytes(b"bbbb")
            with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
                exec(code, scope)

    def test_offline_plan_does_not_call_hosts_or_create_state(self):
        with patch.object(sys, "argv", ["cluster.py", "--config", str(EXAMPLE), "--plan"]), \
                patch.object(cluster.Operator, "remote", side_effect=AssertionError("unexpected SSH")), \
                patch.object(Path, "mkdir", side_effect=AssertionError("unexpected state directory")), \
                patch.object(Path, "write_text", side_effect=AssertionError("unexpected state write")), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            cluster.main()
        self.assertEqual(output.getvalue().count("docker run"), 4)
        self.assertIn("MASTER_ADDR=192.0.2.11", output.getvalue())


if __name__ == "__main__":
    unittest.main()

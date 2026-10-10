#!/usr/bin/env python3
"""Offline checks of stream/token accounting and explicit benchmark operation."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("portable_sparkdash", ROOT / "scripts/run-sparkdash.py")
sparkdash = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sparkdash)


def events(*values, done=True):
    lines = [("data: " + json.dumps(value) + "\r\n\r\n").encode() for value in values]
    if done:
        lines.append(b"data: [DONE]\r\n\r\n")
    return lines


def content(text):
    return {"choices": [{"delta": {"content": text}}]}


def options(**overrides):
    return types.SimpleNamespace(context_window=1048576, serving_lanes=8,
                                 health_mode="required", **overrides)


class SparkDashTests(unittest.TestCase):
    def test_final_usage_is_counted_and_burst_events_are_not_tokens(self):
        stream = events({"choices": [{"delta": {"role": "assistant"}}]},
                        content("first seven tokens in one burst "), content("last burst"),
                        {"choices": [], "usage": {"completion_tokens": 400, "prompt_tokens": 31},
                         "tensorfold": {"decode_s": 3.0}})
        clock = iter([1.0, 2.0, 6.0, 8.0, 9.0])
        result = sparkdash.read_stream(stream, 400, 0.0, lambda: next(clock))
        self.assertEqual(result["content_events"], 2)
        self.assertEqual(result["completion_tokens"], 400)
        self.assertEqual(result["first_content_monotonic"], 2.0)
        self.assertEqual(result["last_content_monotonic"], 6.0)
        self.assertEqual(result["sparkdash_decode_tokens_per_second"], 399 / 4)
        self.assertEqual(result["end_to_end_tokens_per_second"], 400 / 9)
        self.assertEqual(result["server_decode_tokens_per_second"], 400 / 3)
        self.assertEqual(result["ttft_seconds"], 2)

    def test_incomplete_wrong_length_reasoning_and_one_burst_fail(self):
        valid = [content("a"), content("b"), {"usage": {"completion_tokens": 400}, "choices": []}]
        bad_cases = [events(*valid, done=False), events(content("a"), content("b")),
                     events(content("a"), content("b"), {"usage": {"completion_tokens": 399}}),
                     events(content("a"), content("b"), {"choices": [{"delta": {"reasoning_content": "thought"}}], "usage": {"completion_tokens": 400}}),
                     events(content("all tokens"), {"usage": {"completion_tokens": 400}}),
                     events(content("a"), content("b"), {"usage": {"completion_tokens": 400, "completion_tokens_details": {"reasoning_tokens": 1}}})]
        for stream in bad_cases:
            clock = iter(range(1, 20))
            with self.subTest(stream=stream), self.assertRaises(RuntimeError):
                sparkdash.read_stream(stream, 400, 0, lambda: next(clock))

    def test_concurrent_rate_uses_union_wall_span_and_minus_one_per_stream(self):
        requests = [{"first_content_monotonic": 1, "last_content_monotonic": 3,
                     "started_monotonic": 0, "ended_monotonic": 4,
                     "completion_tokens": 100, "ttft_seconds": 1},
                    {"first_content_monotonic": 2, "last_content_monotonic": 6,
                     "started_monotonic": 0.5, "ended_monotonic": 8,
                     "completion_tokens": 200, "ttft_seconds": 1.5}]
        result = sparkdash.phase_metrics(requests)
        self.assertEqual(result["sparkdash_aggregate_decode_tokens_per_second"], (99 + 199) / 5)
        self.assertEqual(result["aggregate_end_to_end_tokens_per_second"], 300 / 8)
        self.assertNotEqual(result["sparkdash_aggregate_decode_tokens_per_second"], 99 / 2 + 199 / 4)

    def test_payload_keeps_sampler_thinking_and_fixed_length_flags(self):
        data = sparkdash.payload("test-model", "prompt", 400)
        self.assertEqual(data, {"model": "test-model", "messages": [{"role": "user", "content": "prompt"}],
                                "temperature": 0, "top_p": 1, "max_tokens": 400, "min_tokens": 400,
                                "ignore_eos": True, "stop": [], "stream": True,
                                "stream_options": {"include_usage": True}, "reasoning_effort": "none",
                                "chat_template_kwargs": {"enable_thinking": False}})

    def test_known_health_mismatch_or_busy_state_always_fails(self):
        health = {"busy": False, "requests_running": 0, "context_length": 1048576,
                  "streams": {"max": 8, "decoding": 0, "prefilling": 0}}
        self.assertTrue(sparkdash.validate_health(health, options())["profile_verified"])
        for change in ({"busy": True}, {"requests_running": 1}, {"context_length": 16384}, {"streams": {"max": 4}}):
            for mode in ("required", "record-only"):
                args = options()
                args.health_mode = mode
                with self.subTest(change=change, mode=mode), self.assertRaises(RuntimeError):
                    sparkdash.validate_health({**health, **change}, args)

    def test_unknown_health_is_explicitly_record_only(self):
        with self.assertRaises(RuntimeError):
            sparkdash.validate_health({}, options())
        args = options()
        args.health_mode = "record-only"
        observed = sparkdash.validate_health({}, args)
        self.assertFalse(observed["profile_verified"])
        self.assertFalse(observed["idle_observed"])

    def test_request_audit_counts_warmups_and_detects_extra_requests(self):
        before = {"snapshot": {"requests_total": 100}}
        after = {"snapshot": {"requests_total": 238}}
        # Three workloads, one warmup each, three repetitions of C1+C2+C4+C8.
        expected = 3 * (1 + 3 * (1 + 2 + 4 + 8))
        self.assertEqual(expected, 138)
        self.assertTrue(sparkdash.request_audit(before, after, expected)["request_count_verified"])
        self.assertTrue(sparkdash.request_audit(before, {"snapshot": {"requests_total": 239}}, expected)["external_requests_or_counter_change_detected"])
        unknown = sparkdash.request_audit({}, {}, expected)
        self.assertIsNone(unknown["requests_delta"])
        self.assertFalse(unknown["request_count_verified"])

    def test_cli_requires_endpoint_and_new_private_output(self):
        with tempfile.TemporaryDirectory(prefix="sparkdash-offline-") as temporary:
            existing = Path(temporary)
            for argv in (["--output", str(existing / "new"), "--plan"],
                         ["--url", "http://example.test:8888", "--plan"],
                         ["--url", "http://example.test:8888", "--output", str(existing), "--plan"],
                         ["--url", "http://example.test:8888", "--output", str(ROOT / "results" / "new-private-run"), "--plan"],
                         ["--url", "http://user:password@example.test:8888", "--output", str(existing / "new"), "--plan"]):
                with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    sparkdash.parse_args(argv)

    def test_plan_never_opens_network_or_writes_files(self):
        with tempfile.TemporaryDirectory(prefix="sparkdash-offline-") as temporary:
            output = Path(temporary) / "new-run"
            with patch.object(sparkdash.urllib.request, "urlopen", side_effect=AssertionError("unexpected HTTP")), \
                    patch.object(Path, "mkdir", side_effect=AssertionError("unexpected directory")), \
                    patch.object(Path, "write_text", side_effect=AssertionError("unexpected state write")), \
                    contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(sparkdash.main(["--url", "http://example.test:8888/v1", "--output", str(output), "--plan"]), 0)
            plan = json.loads(printed.getvalue())
            self.assertEqual(plan["settings"]["expected_inference_requests"], 138)
            self.assertEqual(plan["settings"]["url"], "http://example.test:8888")
            self.assertFalse(output.exists())

    def test_runtime_receipt_checks_profile_but_does_not_claim_live_identity(self):
        args = types.SimpleNamespace(runtime=Path("runtime.json"), qualification=None, metadata=None,
                                     context_window=1048576, serving_lanes=8)
        document = {"passed": True, "hosts": {"rank" + str(rank): {"context": 1048576, "parallel": 8,
                     "container_id": "id" + str(rank)} for rank in range(4)}}
        with patch.object(Path, "read_bytes", return_value=json.dumps(document).encode()):
            _, identity = sparkdash.read_receipts(args)
        self.assertEqual(len(identity["runtime"]["container_ids"]), 4)
        self.assertIn("does not independently verify live", identity["runtime"]["binding_scope"])
        document["hosts"]["rank3"]["parallel"] = 4
        with patch.object(Path, "read_bytes", return_value=json.dumps(document).encode()), self.assertRaises(RuntimeError):
            sparkdash.read_receipts(args)


if __name__ == "__main__":
    unittest.main()

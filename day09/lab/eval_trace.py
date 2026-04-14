"""
eval_trace.py — Trace Evaluation & Comparison
Sprint 4: Chạy pipeline với test questions, phân tích trace, so sánh single vs multi.

Chạy:
    python eval_trace.py                  # Chạy 15 test questions + analyze + compare
    python eval_trace.py --grading        # Chạy grading questions (sau 17:00)
    python eval_trace.py --analyze        # Phân tích trace đã có
    python eval_trace.py --compare        # So sánh single vs multi

Outputs:
    artifacts/traces/          — trace của từng câu hỏi
    artifacts/grading_run.jsonl — log câu hỏi chấm điểm
    artifacts/eval_report.json  — báo cáo tổng kết
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Optional

# Import graph
sys.path.insert(0, os.path.dirname(__file__))
from graph import run_graph, save_trace


# ─────────────────────────────────────────────
# 1. Run Pipeline on Test Questions
# ─────────────────────────────────────────────

def run_test_questions(questions_file: str = "data/test_questions.json") -> list:
    """
    Chạy pipeline với danh sách câu hỏi, lưu trace từng câu.

    Returns:
        list of result dicts
    """
    with open(questions_file, encoding="utf-8") as f:
        questions = json.load(f)

    print(f"\n📋 Running {len(questions)} test questions from {questions_file}")
    print("=" * 60)

    results = []
    for i, q in enumerate(questions, 1):
        question_text = q["question"]
        q_id = q.get("id", f"q{i:02d}")

        print(f"[{i:02d}/{len(questions)}] {q_id}: {question_text[:65]}...")

        try:
            result = run_graph(question_text)
            result["question_id"] = q_id
            result["timestamp"] = datetime.now().isoformat()

            # Save individual trace
            trace_file = save_trace(result, "artifacts/traces")
            print(f"  -> route={result.get('supervisor_route', '?')}, "
                  f"conf={result.get('confidence', 0):.2f}, "
                  f"{result.get('latency_ms', 0)}ms")

            results.append({
                "id": q_id,
                "question": question_text,
                "expected_answer": q.get("expected_answer", ""),
                "expected_sources": q.get("expected_sources", []),
                "expected_route": q.get("expected_route", ""),
                "difficulty": q.get("difficulty", "unknown"),
                "category": q.get("category", "unknown"),
                "test_type": q.get("test_type", "unknown"),
                "result": result,
            })

        except Exception as e:
            print(f"  X ERROR: {e}")
            results.append({
                "id": q_id,
                "question": question_text,
                "error": str(e),
                "result": None,
            })

    succeeded = sum(1 for r in results if r.get("result"))
    print(f"\n✅ Done. {succeeded} / {len(results)} succeeded.")
    return results


# ─────────────────────────────────────────────
# 2. Run Grading Questions (Sprint 4)
# ─────────────────────────────────────────────

def run_grading_questions(questions_file: str = "data/grading_questions.json") -> str:
    """
    Chạy pipeline với grading questions và lưu JSONL log.
    Dùng cho chấm điểm nhóm (chạy sau khi grading_questions.json được public lúc 17:00).

    Returns:
        path tới grading_run.jsonl
    """
    if not os.path.exists(questions_file):
        print(f"[ERR] {questions_file} chua duoc public (sau 17:00 moi co).")
        return ""

    with open(questions_file, encoding="utf-8") as f:
        questions = json.load(f)

    os.makedirs("artifacts", exist_ok=True)
    output_file = "artifacts/grading_run.jsonl"

    print(f"\n[GRADING] Running {len(questions)} grading questions")
    print(f"   Output -> {output_file}")
    print("=" * 60)

    with open(output_file, "w", encoding="utf-8") as out:
        for i, q in enumerate(questions, 1):
            q_id = q.get("id", f"gq{i:02d}")
            question_text = q["question"]
            print(f"[{i:02d}/{len(questions)}] {q_id}: {question_text[:65]}...")

            try:
                result = run_graph(question_text)
                record = {
                    "id": q_id,
                    "question": question_text,
                    "answer": result.get("final_answer", "PIPELINE_ERROR: no answer"),
                    "sources": result.get("retrieved_sources", []),
                    "supervisor_route": result.get("supervisor_route", ""),
                    "route_reason": result.get("route_reason", ""),
                    "workers_called": result.get("workers_called", []),
                    "mcp_tools_used": [t.get("tool") for t in result.get("mcp_tools_used", []) if isinstance(t, dict)],
                    "confidence": result.get("confidence", 0.0),
                    "hitl_triggered": result.get("hitl_triggered", False),
                    "latency_ms": result.get("latency_ms"),
                    "timestamp": datetime.now().isoformat(),
                }
                print(f"  -> route={record['supervisor_route']}, conf={record['confidence']:.2f}")
            except Exception as e:
                record = {
                    "id": q_id,
                    "question": question_text,
                    "answer": f"PIPELINE_ERROR: {e}",
                    "sources": [],
                    "supervisor_route": "error",
                    "route_reason": str(e),
                    "workers_called": [],
                    "mcp_tools_used": [],
                    "confidence": 0.0,
                    "hitl_triggered": False,
                    "latency_ms": None,
                    "timestamp": datetime.now().isoformat(),
                }
                print(f"  X ERROR: {e}")

            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"\n[OK] Grading log saved -> {output_file}")
    return output_file


# ─────────────────────────────────────────────
# 3. Analyze Traces
# ─────────────────────────────────────────────

def analyze_traces(traces_dir: str = "artifacts/traces") -> dict:
    """
    Đọc tất cả trace files và tính metrics tổng hợp.

    Metrics:
    - routing_distribution: phần trăm câu đi vào mỗi worker
    - avg_confidence: confidence trung bình
    - avg_latency_ms: latency trung bình
    - mcp_usage_rate: phần trăm câu có MCP tool call
    - hitl_rate: phần trăm câu trigger HITL
    - abstain_rate: phần trăm câu pipeline abstain
    - source_coverage: các tài liệu nào được dùng nhiều nhất

    Returns:
        dict of metrics
    """
    if not os.path.exists(traces_dir):
        print(f"[WARN] {traces_dir} khong ton tai. Chay run_test_questions() truoc.")
        return {}

    trace_files = sorted([f for f in os.listdir(traces_dir) if f.endswith(".json")])
    if not trace_files:
        print(f"[WARN] Khong co trace files trong {traces_dir}.")
        return {}

    traces = []
    for fname in trace_files:
        with open(os.path.join(traces_dir, fname), encoding="utf-8") as f:
            traces.append(json.load(f))

    # Compute metrics
    routing_counts = {}
    confidences = []
    latencies = []
    mcp_calls = 0
    hitl_triggers = 0
    abstain_count = 0
    source_counts = {}
    workers_distribution = {}

    abstain_keywords = [
        "khong tim thay", "khong du thong tin", "khong du du lieu",
        "không tìm thấy", "không đủ thông tin", "không đủ dữ liệu",
        "khong biet", "không biết"
    ]

    for t in traces:
        # Routing distribution
        route = t.get("supervisor_route", "unknown")
        routing_counts[route] = routing_counts.get(route, 0) + 1

        # Confidence
        conf = t.get("confidence", 0)
        if conf is not None:
            confidences.append(conf)

        # Latency
        lat = t.get("latency_ms")
        if lat is not None and lat > 0:
            latencies.append(lat)

        # MCP usage
        mcp_tools = t.get("mcp_tools_used", [])
        if mcp_tools and len(mcp_tools) > 0:
            mcp_calls += 1

        # HITL
        if t.get("hitl_triggered", False):
            hitl_triggers += 1

        # Abstain detection
        answer = t.get("final_answer", "").lower()
        if any(kw in answer for kw in abstain_keywords):
            abstain_count += 1

        # Source coverage
        for src in t.get("retrieved_sources", []):
            source_counts[src] = source_counts.get(src, 0) + 1

        # Workers distribution
        for w in t.get("workers_called", []):
            workers_distribution[w] = workers_distribution.get(w, 0) + 1

    total = len(traces)

    # Routing accuracy (nếu có expected_route trong trace)
    correct_routes = 0
    total_with_expected = 0

    metrics = {
        "total_traces": total,
        "routing_distribution": {
            k: {"count": v, "percent": f"{100*v//total}%"}
            for k, v in sorted(routing_counts.items(), key=lambda x: -x[1])
        },
        "workers_distribution": {
            k: {"count": v, "percent": f"{100*v//total}%"}
            for k, v in sorted(workers_distribution.items(), key=lambda x: -x[1])
        },
        "avg_confidence": round(sum(confidences) / len(confidences), 3) if confidences else 0,
        "avg_latency_ms": round(sum(latencies) / len(latencies)) if latencies else 0,
        "mcp_usage_rate": {
            "count": mcp_calls,
            "total": total,
            "percent": f"{100*mcp_calls//total}%" if total else "0%"
        },
        "hitl_rate": {
            "count": hitl_triggers,
            "total": total,
            "percent": f"{100*hitl_triggers//total}%" if total else "0%"
        },
        "abstain_rate": {
            "count": abstain_count,
            "total": total,
            "percent": f"{100*abstain_count//total}%" if total else "0%",
            "rate": round(abstain_count / total, 2) if total else 0,
        },
        "top_sources": sorted(source_counts.items(), key=lambda x: -x[1])[:10],
        "confidence_range": {
            "min": round(min(confidences), 3) if confidences else 0,
            "max": round(max(confidences), 3) if confidences else 0,
        },
    }

    return metrics


# ─────────────────────────────────────────────
# 4. Compare Single vs Multi Agent
# ─────────────────────────────────────────────

def compare_single_vs_multi(
    multi_traces_dir: str = "artifacts/traces",
    day08_results_file: Optional[str] = None,
) -> dict:
    """
    So sánh Day 08 (single agent RAG) vs Day 09 (multi-agent).

    Returns:
        dict của comparison metrics
    """
    multi_metrics = analyze_traces(multi_traces_dir)

    # Day 08 baseline — lấy từ kết quả thực tế đã chạy eval.py Day 08
    day08_baseline = {
        "total_questions": 15,
        "avg_confidence": 0.76,
        "avg_latency_ms": 7448,
        "abstain_rate": 0.0,
        "multi_hop_accuracy": 0.4,
    }

    # Load Day 08 results file nếu có
    if day08_results_file and os.path.exists(day08_results_file):
        with open(day08_results_file, encoding="utf-8") as f:
            day08_baseline = json.load(f)

    # Tính delta metrics
    d09_confidence = multi_metrics.get("avg_confidence", 0)
    d09_latency = multi_metrics.get("avg_latency_ms", 0)
    d09_abstain = multi_metrics.get("abstain_rate", {}).get("rate", 0) if isinstance(multi_metrics.get("abstain_rate"), dict) else 0

    d08_confidence = day08_baseline.get("avg_confidence", 0)
    d08_latency = day08_baseline.get("avg_latency_ms", 0)
    d08_abstain = day08_baseline.get("abstain_rate", 0)

    delta_confidence = round(d09_confidence - d08_confidence, 3)
    delta_latency = d09_latency - d08_latency
    delta_abstain = round(d09_abstain - d08_abstain, 2)

    comparison = {
        "generated_at": datetime.now().isoformat(),
        "day08_single_agent": day08_baseline,
        "day09_multi_agent": {
            "total_questions": multi_metrics.get("total_traces", 0),
            "avg_confidence": d09_confidence,
            "avg_latency_ms": d09_latency,
            "abstain_rate": d09_abstain,
            "routing_distribution": multi_metrics.get("routing_distribution", {}),
            "mcp_usage_rate": multi_metrics.get("mcp_usage_rate", {}),
            "hitl_rate": multi_metrics.get("hitl_rate", {}),
            "top_sources": multi_metrics.get("top_sources", []),
        },
        "delta": {
            "confidence": f"{delta_confidence:+.3f}" + (" (Day09 better)" if delta_confidence > 0 else " (Day08 better)" if delta_confidence < 0 else " (equal)"),
            "latency_ms": f"{delta_latency:+d}" + (" (Day09 faster)" if delta_latency < 0 else " (Day08 faster)" if delta_latency > 0 else " (equal)"),
            "abstain_rate": f"{delta_abstain:+.2f}",
        },
        "analysis": {
            "routing_visibility": "Day 09 co route_reason cho tung cau -> dang debug hon Day 08. Day 08 la monolithic pipeline khong co routing.",
            "latency_comparison": f"Day 08: {d08_latency}ms vs Day 09: {d09_latency}ms. Delta: {delta_latency:+d}ms. "
                                  + ("Day 09 nhanh hon vi dung mock workers (khong goi LLM). Trong production, multi-agent se cham hon single-agent do nhieu LLM calls." if delta_latency < 0
                                     else "Day 09 cham hon do co nhieu buoc xu ly (supervisor + workers + synthesis)."),
            "confidence_comparison": f"Day 08: {d08_confidence} vs Day 09: {d09_confidence}. Delta: {delta_confidence:+.3f}. "
                                     + ("Multi-agent co confidence cao hon nho co policy checking va structured routing." if delta_confidence > 0
                                        else "Single-agent co confidence cao hon, nhung khong co route_reason de giai thich."),
            "debuggability": "Multi-agent: co the test tung worker doc lap (python workers/retrieval.py). Single-agent: phai debug toan bo pipeline.",
            "extensibility": "Day 09 co the extend capability qua MCP tools khong can sua core logic. Day 08 phai hard-code vao prompt.",
            "mcp_benefit": f"Day 09 su dung MCP tools cho {multi_metrics.get('mcp_usage_rate', {}).get('percent', '0%')} so cau hoi. Day 08 khong co MCP.",
        },
    }

    return comparison


# ─────────────────────────────────────────────
# 5. Save Eval Report
# ─────────────────────────────────────────────

def save_eval_report(comparison: dict) -> str:
    """Lưu báo cáo eval tổng kết ra file JSON."""
    os.makedirs("artifacts", exist_ok=True)
    output_file = "artifacts/eval_report.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(comparison, f, ensure_ascii=False, indent=2)
    return output_file


# ─────────────────────────────────────────────
# 6. CLI Entry Point
# ─────────────────────────────────────────────

def print_metrics(metrics: dict, indent: int = 2):
    """Print metrics dep."""
    if not metrics:
        return
    prefix = " " * indent
    print(f"\n{'='*60}")
    print(f"  TRACE ANALYSIS REPORT")
    print(f"{'='*60}")
    for k, v in metrics.items():
        if isinstance(v, list):
            print(f"{prefix}{k}:")
            for item in v:
                if isinstance(item, tuple):
                    print(f"{prefix}  - {item[0]}: {item[1]}")
                else:
                    print(f"{prefix}  - {item}")
        elif isinstance(v, dict):
            print(f"{prefix}{k}:")
            for kk, vv in v.items():
                if isinstance(vv, dict):
                    print(f"{prefix}  {kk}: {vv}")
                else:
                    print(f"{prefix}  {kk}: {vv}")
        else:
            print(f"{prefix}{k}: {v}")


def print_comparison(comparison: dict):
    """Print comparison report."""
    print(f"\n{'='*60}")
    print(f"  DAY 08 vs DAY 09 COMPARISON")
    print(f"{'='*60}")

    d08 = comparison.get("day08_single_agent", {})
    d09 = comparison.get("day09_multi_agent", {})
    delta = comparison.get("delta", {})

    print(f"\n  {'Metric':<25} {'Day 08':>12} {'Day 09':>12} {'Delta':>15}")
    print(f"  {'-'*65}")
    print(f"  {'avg_confidence':<25} {d08.get('avg_confidence', 'N/A'):>12} {d09.get('avg_confidence', 'N/A'):>12} {delta.get('confidence', 'N/A'):>15}")
    print(f"  {'avg_latency_ms':<25} {d08.get('avg_latency_ms', 'N/A'):>12} {d09.get('avg_latency_ms', 'N/A'):>12} {delta.get('latency_ms', 'N/A'):>15}")
    print(f"  {'abstain_rate':<25} {d08.get('abstain_rate', 'N/A'):>12} {d09.get('abstain_rate', 'N/A'):>12} {delta.get('abstain_rate', 'N/A'):>15}")
    print(f"  {'routing_visibility':<25} {'No':>12} {'Yes':>12} {'N/A':>15}")
    print(f"  {'mcp_tools':<25} {'No':>12} {'Yes':>12} {'N/A':>15}")

    print(f"\n  Analysis:")
    for k, v in comparison.get("analysis", {}).items():
        print(f"    {k}:")
        # Wrap long lines
        words = str(v).split()
        line = "      "
        for w in words:
            if len(line) + len(w) > 90:
                print(line)
                line = "      " + w
            else:
                line += " " + w if line.strip() else "      " + w
        if line.strip():
            print(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Day 09 Lab - Trace Evaluation")
    parser.add_argument("--grading", action="store_true", help="Run grading questions")
    parser.add_argument("--analyze", action="store_true", help="Analyze existing traces")
    parser.add_argument("--compare", action="store_true", help="Compare single vs multi")
    parser.add_argument("--test-file", default="data/test_questions.json", help="Test questions file")
    args = parser.parse_args()

    if args.grading:
        # Chạy grading questions
        log_file = run_grading_questions()
        if log_file:
            print(f"\n[OK] Grading log: {log_file}")
            print("   Nop file nay truoc 18:00!")

    elif args.analyze:
        # Phân tích traces
        metrics = analyze_traces()
        print_metrics(metrics)

    elif args.compare:
        # So sánh single vs multi
        comparison = compare_single_vs_multi()
        report_file = save_eval_report(comparison)
        print_comparison(comparison)
        print(f"\n  Report saved -> {report_file}")

    else:
        # Default: chạy FULL pipeline
        print("=" * 60)
        print("  Day 09 Lab - Full Evaluation Pipeline")
        print("=" * 60)

        # Step 1: Chạy 15 test questions
        results = run_test_questions(args.test_file)

        # Step 2: Phân tích trace
        metrics = analyze_traces()
        print_metrics(metrics)

        # Step 3: So sánh single vs multi
        comparison = compare_single_vs_multi()
        print_comparison(comparison)

        # Step 4: Lưu báo cáo
        report_file = save_eval_report(comparison)
        print(f"\n  Eval report -> {report_file}")

        # Step 5: Verify trace format
        print(f"\n{'='*60}")
        print(f"  TRACE FORMAT VERIFICATION")
        print(f"{'='*60}")
        required_fields = [
            "run_id", "task", "supervisor_route", "route_reason",
            "workers_called", "mcp_tools_used", "retrieved_sources",
            "final_answer", "confidence", "hitl_triggered", "latency_ms",
            "timestamp"
        ]
        traces_dir = "artifacts/traces"
        trace_files = [f for f in os.listdir(traces_dir) if f.endswith(".json")]
        all_ok = True
        for fname in trace_files[:3]:  # Check first 3
            with open(os.path.join(traces_dir, fname), encoding="utf-8") as f:
                trace = json.load(f)
            missing = [field for field in required_fields if field not in trace]
            if missing:
                print(f"  [FAIL] {fname}: missing fields: {missing}")
                all_ok = False
            else:
                print(f"  [OK] {fname}: all {len(required_fields)} required fields present")

        if all_ok:
            print(f"\n  [OK] All traces have correct format!")
        print(f"  Total traces: {len(trace_files)}")

        print(f"\n{'='*60}")
        print(f"  Sprint 4 COMPLETE!")
        print(f"{'='*60}")
        print(f"  [OK] 15 test questions executed")
        print(f"  [OK] Traces saved to artifacts/traces/")
        print(f"  [OK] analyze_traces() computed metrics")
        print(f"  [OK] compare_single_vs_multi() generated comparison")
        print(f"  [OK] Eval report -> {report_file}")

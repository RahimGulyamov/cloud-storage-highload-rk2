#!/usr/bin/env python3
"""Capacity model for the architecture report; Python 3, standard library only.

Numbers are engineering assumptions, not load-test measurements. The README's
marked tables are generated from this model so prose and arithmetic can be audited.
"""
import argparse
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAY = 86400
TB = 10**12


def fmt(value, places=2):
    return f"{value:,.{places}f}".replace(",", " ")


def table(headers, rows):
    return "\n".join([
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
        *("| " + " | ".join(map(str, row)) + " |" for row in rows),
    ])


def calculate(p):
    dau = p["mau"] * p["dau_mau_ratio"]
    actions = []
    for a in p["actions"]:
        n = dau * a["per_dau"]
        requests = n * a["api_requests"]
        actions.append(dict(a, actions_day=n, requests_day=requests,
                            avg_rps=requests / DAY,
                            peak_rps=requests / DAY * p["peak_factor"]))
    by_key = {a["key"]: a for a in actions}
    uploads = by_key["upload"]["actions_day"]
    # Conservatively treat every delete/restore action as a new deletion.
    deletes = by_key["delete"]["actions_day"]
    overwritten = uploads * (1 - p["new_file_fraction_of_uploads"])
    new_files = uploads * p["new_file_fraction_of_uploads"]
    live_count = p["registered_users"] * p["live_files_per_registered_user"]
    size = p["mean_file_bytes"]
    version_count = overwritten * p["retention_days"]
    trash_count = deletes * p["retention_days"]
    preview_count = live_count * p["preview_fraction"]
    blob_rows = [
        ("Оригиналы, текущие версии", live_count, live_count * size),
        ("Предыдущие версии, 30 суток", version_count, version_count * size),
        ("Корзина, 30 суток", trash_count, trash_count * size),
        ("Превью JPEG/PNG", preview_count, preview_count * p["mean_preview_bytes"]),
        ("Multipart / карантин, бюджет 2 суток входа", uploads * p["temporary_ingress_days"],
         uploads * size * p["temporary_ingress_days"]),
    ]
    logical_blob = sum(row[2] for row in blob_rows)
    # Mean row lengths include tuples; the factor separately budgets indexes.
    mutation_events_day = sum(by_key[k]["actions_day"]
                              for k in ["upload", "folder", "share", "delete"])
    critical_events_day = mutation_events_day + uploads + uploads * p["preview_fraction"]
    idempotent_requests_day = (uploads * 2 + by_key["folder"]["actions_day"]
                              + by_key["share"]["actions_day"] + deletes)
    metadata_rows = [
        ("Профили и учёт квоты", p["registered_users"], 1000),
        ("Каталоги", p["registered_users"] * p["folders_per_registered_user"], 500),
        ("Файлы: текущие и tombstones корзины", live_count + trash_count, 1000),
        ("Версии: текущие + старые + корзина", live_count + version_count + trash_count, 500),
        ("Общие ссылки, до 10% текущих файлов", live_count * 0.1, 500),
        ("Upload sessions, 1 сутки", uploads, 500),
        ("Idempotency keys, 2 суток", idempotent_requests_day * 2, 500),
        ("Outbox, 7 суток", critical_events_day * 7, 500),
        ("Inbox четырёх обработчиков, 7 суток", critical_events_day * 7 * 4, 100),
    ]
    metadata_heap = sum(n * size for _, n, size in metadata_rows)
    api_in = sum(a["requests_day"] * a["request_bytes"] for a in actions)
    api_out = sum(a["requests_day"] * a["response_bytes"] for a in actions)
    upload_bytes = uploads * size
    download_bytes = by_key["download"]["actions_day"] * size
    preview_bytes = by_key["preview"]["actions_day"] * p["mean_preview_bytes"]
    traffic_rows = [
        ("Вход: файлы пользователей", upload_bytes),
        ("Вход: тела control API", api_in),
        ("Выход: скачивание оригиналов", download_bytes),
        ("Выход: скачивание превью", preview_bytes),
        ("Выход: тела control API", api_out),
    ]
    raw_capacity = p["storage_hosts"] * p["hdd_per_host"] * p["hdd_bytes"]
    physical_blob = logical_blob * p["replication_factor"] * p["object_overhead_factor"]
    wire = p["wire_overhead_factor"]
    peak = p["peak_factor"]
    growth = p["planning_growth_factor"]
    def gbps(n, peak_factor=1):
        return n / DAY * 8 / 10**9 * peak_factor * wire
    s3_rows = [
        ("CreateMultipartUpload, backend", uploads),
        ("UploadPart, клиенты", uploads * p["multipart_parts_per_upload"]),
        ("CompleteMultipartUpload, backend", uploads),
        ("HEAD после завершения, backend", uploads),
        ("GET оригиналов, клиенты", by_key["download"]["actions_day"]),
        ("GET превью, клиенты", by_key["preview"]["actions_day"]),
        ("GET: scan + preview + DR copy", uploads * (2 + p["preview_fraction"])),
        ("PUT готовых превью", uploads * p["preview_fraction"]),
        ("DELETE по истечении срока хранения", overwritten + deletes),
    ]
    # Kafka: domain changes + scan results + preview results + successful reads.
    event_day = (critical_events_day
                 + by_key["download"]["actions_day"] + by_key["preview"]["actions_day"])
    backup_logical = (live_count + version_count + trash_count) * size * 1.2
    metrics = {
        "dau": dau, "actions": actions, "blob_rows": blob_rows,
        "metadata_rows": metadata_rows, "traffic_rows": traffic_rows,
        "s3_rows": s3_rows,
        "api_requests_day": sum(a["requests_day"] for a in actions),
        "api_avg_rps": sum(a["avg_rps"] for a in actions),
        "api_peak_rps": sum(a["peak_rps"] for a in actions),
        "s3_requests_day": sum(n for _, n in s3_rows),
        "s3_peak_rps": sum(n for _, n in s3_rows) / DAY * peak,
        "registered_users": p["registered_users"],
        "mau": p["mau"],
        "activity_days_per_mau_month": dau * p["days_per_month"] / p["mau"],
        "logical_blob_tb": logical_blob / TB,
        "physical_blob_tb": physical_blob / TB,
        "year1_physical_blob_tb": physical_blob * growth / TB,
        "raw_capacity_tb": raw_capacity / TB,
        "base_storage_utilization": physical_blob / raw_capacity,
        "year1_storage_utilization": physical_blob * growth / raw_capacity,
        "year1_degraded_storage_utilization": (
            physical_blob * growth * (p["replication_factor"] - 1) / p["replication_factor"]
            / (raw_capacity * (p["availability_zones"] - 1) / p["availability_zones"])),
        "metadata_heap_tb": metadata_heap / TB,
        "metadata_indexed_tb": metadata_heap * p["metadata_index_factor"] / TB,
        "year1_metadata_indexed_tb": metadata_heap * p["metadata_index_factor"] * growth / TB,
        "in_gbps_avg": gbps(upload_bytes + api_in),
        "out_gbps_avg": gbps(download_bytes + preview_bytes + api_out),
        "in_gbps_peak": gbps(upload_bytes + api_in, peak),
        "out_gbps_peak": gbps(download_bytes + preview_bytes + api_out, peak),
        "year1_out_gbps_peak": gbps(download_bytes + preview_bytes + api_out, peak) * growth,
        "new_files_day": new_files, "overwrites_day": overwritten,
        "deletes_day": deletes, "mutation_events_day": mutation_events_day,
        "critical_events_day": critical_events_day,
        "net_live_tb_year": (new_files - deletes) * size * p["days_per_year"] / TB,
        "year1_live_tb_by_flow": (live_count * size + (new_files-deletes)*size*p["days_per_year"]) / TB,
        "kafka_events_day": event_day,
        "kafka_peak_events_sec": event_day / DAY * peak,
        "kafka_7d_replicated_tb": event_day * 1000 * 7 * 3 / TB,
        "clickhouse_90d_replicated_tb": event_day * 400 / 4 * 90 * 3 / TB,
        "backup_logical_tb": backup_logical / TB,
        "year1_backup_logical_tb": backup_logical * growth / TB,
        "scan_peak_mb_sec_year1": upload_bytes / DAY * peak * growth / 10**6,
        "scan_required_cores_at_60_percent": math.ceil(upload_bytes / DAY * peak * growth / (20*10**6*0.6)),
        "preview_peak_images_sec_year1": uploads * p["preview_fraction"] / DAY * peak * growth,
    }
    metrics["preview_required_cores_at_60_percent"] = (
        metrics["preview_peak_images_sec_year1"] * 0.08 / 0.6)
    return metrics


def sections(p, m):
    actions = m["actions"]
    workload = table(
        ["Тип control API", "Действий / DAU / сутки", "HTTP / действие", "HTTP / сутки", "Средний RPS", "Пиковый RPS (×6)"],
        [[a["name"], fmt(a["per_dau"],1), a["api_requests"], fmt(a["requests_day"],0),
          fmt(a["avg_rps"]), fmt(a["peak_rps"])] for a in actions]
        + [["**Всего**", "", "", fmt(m["api_requests_day"],0), fmt(m["api_avg_rps"]), fmt(m["api_peak_rps"])]]
    )
    blob = table(
        ["Данные", "Логических объектов", "Одна копия, ТБ", "3 копии + 10%, ТБ", "Через год, ×1.5, ТБ физических"],
        [[name,fmt(n,0),fmt(b/TB),fmt(b/TB*3*1.1),fmt(b/TB*3*1.1*1.5)]
         for name,n,b in m["blob_rows"]]
        + [["**Итого**", fmt(sum(n for _,n,_ in m["blob_rows"]),0),fmt(m["logical_blob_tb"]),
            fmt(m["physical_blob_tb"]),fmt(m["year1_physical_blob_tb"])]]
    )
    meta = table(
        ["Категория PostgreSQL", "Записей", "Средний размер строки, байт", "Heap, ТБ"],
        [[name,fmt(n,0),size,fmt(n*size/TB,6)] for name,n,size in m["metadata_rows"]]
        + [["**Всего до индексов**","","",fmt(m["metadata_heap_tb"],6)],
           ["**Heap + индексы (×2)**","","",fmt(m["metadata_indexed_tb"],6)]]
    )
    traffic = table(
        ["Поток", "Полезных ТБ / сутки", "Средних Гбит/с, +15%", "Пиковых Гбит/с, ×6 и +15%"],
        [[name,fmt(b/TB,6),fmt(b/DAY*8/1e9*1.15,4),fmt(b/DAY*8/1e9*1.15*6,4)]
         for name,b in m["traffic_rows"]]
        + [["**Вход, всего**", "",fmt(m["in_gbps_avg"],4),fmt(m["in_gbps_peak"],4)],
           ["**Выход, всего**", "",fmt(m["out_gbps_avg"],4),fmt(m["out_gbps_peak"],4)]]
    )
    s3 = table(
        ["Тип запросов RGW/S3", "Запросов / сутки", "Пиковый RPS, ×6"],
        [[name,fmt(n,0),fmt(n/DAY*6)] for name,n in m["s3_rows"]]
        + [["**Всего**",fmt(m["s3_requests_day"],0),fmt(m["s3_peak_rps"])]]
    )
    growth = table(
        ["Сценарий", "Control API, пик RPS", "S3, пик RPS", "Выход наружу, Гбит/с", "Физические файлы, ТБ", "Загрузка HDD-пула"],
        [[label,fmt(m["api_peak_rps"]*factor),fmt(m["s3_peak_rps"]*factor),
          fmt(m["out_gbps_peak"]*factor,3),fmt(m["physical_blob_tb"]*factor),
          fmt(m["physical_blob_tb"]*factor/m["raw_capacity_tb"]*100)+"%"]
         for label,factor in [("Базовый, 1 млн MAU",1),("Годовой запас, ×1.5",1.5),("Чувствительность, ×2",2)]]
    )
    return dict(WORKLOAD=workload, BLOBS=blob, METADATA=meta, TRAFFIC=traffic, S3=s3, GROWTH=growth)


def validate(p, m):
    checks = {
        "MAU at least one million": p["mau"] >= 1000000,
        "DAU <= MAU <= registered": 0 < m["dau"] <= p["mau"] <= p["registered_users"],
        "all AZs have same host count": p["storage_hosts"] % p["availability_zones"] == 0,
        "one replica per AZ is possible": p["replication_factor"] == p["availability_zones"],
        "year-one physical data fits below 70%": m["year1_storage_utilization"] < p["target_max_storage_utilization"],
        "zone failure leaves 2 copies below 70%": m["year1_degraded_storage_utilization"] < 0.7,
        "traffic fits a surviving 10Gbps edge": m["year1_out_gbps_peak"] < 10,
        "API at year one fits 6 surviving 250RPS replicas": m["api_peak_rps"]*1.5 < 6*250,
        "scan at year one fits 24 surviving cores": m["scan_required_cores_at_60_percent"] <= 24,
        "preview at year one fits 4 surviving cores": m["preview_required_cores_at_60_percent"] <= 4,
        "day-flow live growth is below 50% budget": m["year1_live_tb_by_flow"] < m["blob_rows"][0][2]/TB*1.5,
        "PG data + 20% bloat + 200GB WAL fits 2TB": m["year1_metadata_indexed_tb"]*1.2+0.2 < 2,
        "three-copy Kafka + growth + double retry budget fits 3x0.5TB": m["kafka_7d_replicated_tb"]*1.5*2 < 1.5,
        "backup budget fits 1.6PB": m["year1_backup_logical_tb"] < 1600,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise SystemExit("Capacity checks failed: " + "; ".join(failed))
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--update-readme", action="store_true")
    parser.add_argument("--check", action="store_true", help="fail on stale generated output")
    args = parser.parse_args()
    p = json.loads((ROOT/"model/assumptions.json").read_text())
    m = calculate(p)
    checks = validate(p,m)
    rendered = sections(p,m)
    output = json.dumps({"assumptions": p, "metrics": m, "checks": checks},
                        ensure_ascii=False, indent=2) + "\n"
    target = ROOT/"model/results.json"
    if args.check:
        if not target.exists() or target.read_text() != output:
            raise SystemExit("model/results.json is stale; run model/calculate.py")
    else:
        target.write_text(output)
    readme = ROOT/"README.md"
    if args.update_readme or args.check:
        doc = readme.read_text()
        for key, fragment in rendered.items():
            start = f"<!-- BEGIN GENERATED {key} -->"
            end = f"<!-- END GENERATED {key} -->"
            before, rest = doc.split(start,1)
            current, after = rest.split(end,1)
            replacement = "\n" + fragment + "\n"
            if args.check and current != replacement:
                raise SystemExit(f"README table {key} is stale")
            doc = before + start + replacement + end + after
        if args.update_readme:
            readme.write_text(doc)
    print(json.dumps({k:m[k] for k in ["api_requests_day","api_peak_rps","s3_peak_rps",
          "logical_blob_tb","physical_blob_tb","year1_physical_blob_tb","metadata_indexed_tb",
          "in_gbps_peak","out_gbps_peak","kafka_events_day","kafka_7d_replicated_tb",
          "clickhouse_90d_replicated_tb","year1_backup_logical_tb"]},ensure_ascii=False,indent=2))
    print(f"PASS: {len(checks)} meaningful capacity checks" + ("; generated tables are current" if args.check else ""))


if __name__ == "__main__":
    main()

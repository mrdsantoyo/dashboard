import csv
import os
import random
from datetime import date, timedelta

ROOT_OUT = "data"
os.makedirs(ROOT_OUT, exist_ok=True)

random.seed(42)  

CHANNELS = [
    {
        "id": 1,
        "name": "Programmatic",
        "type": "paid",
        "ctr_mean": 0.04,
        "cpc_mean": 1.10,
    },
    {
        "id": 2,
        "name": "Paid Search",
        "type": "paid",
        "ctr_mean": 0.05,
        "cpc_mean": 1.30,
    },
    {
        "id": 3,
        "name": "Paid Social",
        "type": "paid",
        "ctr_mean": 0.03,
        "cpc_mean": 0.90,
    },
    {
        "id": 4, 
        "name": 
            "Organic", 
            "type": "organic", 
            "ctr_mean": 0.01, 
            "cpc_mean": 0.00
    },
]

DATA_SOURCES = [
    {"id": 1, "name": "Amazon Ad server", "platform": "Amazon"},
    {"id": 2, "name": "Stackadapt", "platform": "StackAdapt"},
    {"id": 3, "name": "LinkedIn Ads", "platform": "LinkedIn"},
    {"id": 4, "name": "Facebook", "platform": "Facebook"},
    {"id": 5, "name": "Google Ads", "platform": "DisplayVideo360"},
    {"id": 6, "name": "Bing Ads", "platform": "Bing"},
    {"id": 7, "name": "Google search Ads 360", "platform": "GoogleSearch360"},
]

CAMPAIGNS = [
    {
        "id": 1,
        "name": "Persistent 24/7 attitude",
        "channel_id": 1,
        "data_source_id": 1,
        "start_date": date(2024, 1, 1),
        "end_date": date(2025, 12, 31),
        "baseline_impressions_per_day": 9000,
        "avg_order_value": 80.0,
        "budget": 250000.0,
    },
    {
        "id": 2,
        "name": "Cross-platfform static hierarchy",
        "channel_id": 1,
        "data_source_id": 1,
        "start_date": date(2025, 3, 1),
        "end_date": date(2025, 7, 31),
        "baseline_impressions_per_day": 9000,
        "avg_order_value": 80.0,
        "budget": 250000.0,
    },
    {
        "id": 3,
        "name": "Integrated Dedicated Contency",
        "channel_id": 2,
        "data_source_id": 7,
        "start_date": date(2024, 1, 1),
        "end_date": date(2025, 12, 31),
        "baseline_impressions_per_day": 7000,
        "avg_order_value": 110.0,
        "budget": 300000.0,
    },
    {
        "id": 4,
        "name": "Networked valued-added tf",
        "channel_id": 4,
        "data_source_id": 5,
        "start_date": date(2024, 1, 1),
        "end_date": date(2025, 12, 31),
        "baseline_impressions_per_day": 8000,
        "avg_order_value": 70.0,
        "budget": 280000.0,
    },
    {
        "id": 5,
        "name": "Black Friday 2024",
        "channel_id": 3,
        "data_source_id": 5,
        "start_date": date(2024, 11, 29),
        "end_date": date(2024, 11, 30),
        "baseline_impressions_per_day": 9400,
        "avg_order_value": 70.0,
        "budget": 60000.0,
    },    
    {
        "id": 6,
        "name": "Black Friday 2025",
        "channel_id": 4,
        "data_source_id": 5,
        "start_date": date(2025, 11, 28),
        "end_date": date(2025, 11, 29),
        "baseline_impressions_per_day": 1000,
        "avg_order_value": 70.0,
        "budget": 580000.0,
    },
    {
        "id": 7,
        "name": "Summer 2025",
        "channel_id": 3,
        "data_source_id": 5,
        "start_date": date(2025, 6, 28),
        "end_date": date(2025, 8, 29),
        "baseline_impressions_per_day": 7500,
        "avg_order_value": 94.0,
        "budget": 500000.0,
    },
]

START = date(2024, 1, 1)
END = date(2025, 12, 31)


def daterange(start_dt: date, end_dt: date):
    cur = start_dt
    while cur <= end_dt:
        yield cur
        cur = cur + timedelta(days=1)


def write_csv(path: str, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def generate():
    # 2) Campaigns mapping (3 entries) ya definidos en CAMPAIGNS
    # 3) Generación diaria
    daily_rows = []

    days = list(daterange(START, END))
    for d in days:
        for cam in CAMPAIGNS:
            ch = next((c for c in CHANNELS if c["id"] == cam["channel_id"]), None)
            ds = next(
                (s for s in DATA_SOURCES if s["id"] == cam["data_source_id"]), None
            )
            if not ch or not ds:
                continue
            # Impressions base y ruido
            base_impr = cam["baseline_impressions_per_day"]
            impr = max(0, int(random.gauss(base_impr, base_impr * 0.25)))

            # CTR y CPC
            ctr = random.gauss(ch["ctr_mean"], max(0.0001, ch["ctr_mean"] * 0.25))
            ctr = max(0.001, min(0.30, ctr))
            cpc = max(
                0.1, random.gauss(ch["cpc_mean"], max(0.01, ch["cpc_mean"] * 0.3))
            )

            # Interacciones
            clicks = int(impr * ctr)
            spend = clicks * cpc

            # Conversiones y revenue
            conv_rate = 0.01 + random.random() * 0.04  # 1% - 5%
            conversions = int(clicks * conv_rate)
            revenue = conversions * cam["avg_order_value"]

            # Video views
            video_view_factor = (
                0.02
                if "video" in ds["name"].lower() or "video" in ch["name"].lower()
                else 0.01
            )
            daily_views = int(impr * video_view_factor)

            # CPM y conversion rate
            cpm = (spend / impr * 1000) if impr > 0 else 0.0
            conversion_rate = conversions / max(clicks, 1)

            daily_rows.append(
                [
                    d.isoformat(),
                    cam["id"],
                    cam["channel_id"],
                    cam["data_source_id"],
                    impr,
                    clicks,
                    spend,
                    revenue,
                    cpm,
                    ctr,
                    cpc,
                    daily_views,
                    conversions,
                    conversion_rate,
                ]
            )

    # 4) Exportar CSV
    write_csv(
        os.path.join(ROOT_OUT, "channels.csv"),
        ["id", "name", "type", "ctr_mean", "cpc_mean"],
        [
            [c["id"], c["name"], c["type"], c["ctr_mean"], c["cpc_mean"]]
            for c in CHANNELS
        ],
    )

    write_csv(
        os.path.join(ROOT_OUT, "data_sources.csv"),
        ["id", "name", "platform"],
        [[ds["id"], ds["name"], ds["platform"]] for ds in DATA_SOURCES],
    )

    write_csv(
        os.path.join(ROOT_OUT, "campaigns.csv"),
        [
            "id",
            "name",
            "channel_id",
            "data_source_id",
            "start_date",
            "end_date",
            "baseline_impressions_per_day",
            "avg_order_value",
            "budget",
        ],
        [
            [
                c["id"],
                c["name"],
                c["channel_id"],
                c["data_source_id"],
                c["start_date"].isoformat(),
                c["end_date"].isoformat(),
                c["baseline_impressions_per_day"],
                c["avg_order_value"],
                c["budget"],
            ]
            for c in CAMPAIGNS
        ],
    )

    write_csv(
        os.path.join(ROOT_OUT, "daily.csv"),
        [
            "date",
            "campaign_id",
            "channel_id",
            "data_source_id",
            "impressions",
            "clicks",
            "spend",
            "revenue",
            "cpm",
            "ctr",
            "cpc",
            "daily_views",
            "conversions",
            "conversion_rate",
        ],
        daily_rows,
    )

    print("CSV generation complete. Files are in ./data/")


if __name__ == "__main__":
    generate()

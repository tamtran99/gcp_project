"""Sinh bộ dữ liệu e-commerce mẫu ra data/raw/*.csv.

Dữ liệu được cố ý làm "bẩn" một chút (email viết hoa lẫn lộn, khoảng
trắng thừa, giá trị rỗng, đơn hàng bị huỷ) để tầng staging của dbt có
việc thật sự phải làm.

Chỉ dùng thư viện chuẩn của Python — không cần cài thêm gì.

Chạy:  python scripts/generate_sample_data.py
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

# Seed cố định -> chạy lại luôn ra cùng một bộ dữ liệu
RANDOM_SEED = 42

N_CUSTOMERS = 500
N_PRODUCTS = 60
N_ORDERS = 4_000

# Dữ liệu trải 24 tháng gần nhất TÍNH TỚI HÔM NAY, không phải ngày cố định.
# Lý do: dim_customers tính phân khúc churn bằng
# date_diff(current_date(), last_order_date). Nếu cố định mốc thời gian,
# chỉ sau vài tháng là toàn bộ khách hàng đều rơi vào 'churned' và cột
# customer_segment mất hết ý nghĩa.
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=730)

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

FIRST_NAMES = [
    "An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hieu", "Khanh", "Lan",
    "Minh", "Nam", "Oanh", "Phuc", "Quan", "Son", "Thao", "Trang", "Tuan",
    "Vy", "Yen",
]
LAST_NAMES = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Dang", "Bui",
    "Do", "Ho", "Ngo", "Duong", "Ly",
]
CITIES = [
    ("VN", "Ho Chi Minh City"), ("VN", "Ha Noi"), ("VN", "Da Nang"),
    ("VN", "Can Tho"), ("SG", "Singapore"), ("TH", "Bangkok"),
    ("MY", "Kuala Lumpur"), ("ID", "Jakarta"), ("PH", "Manila"),
]
CATEGORIES = {
    "Electronics": ["Aurora", "Nexpo", "Volt"],
    "Home & Kitchen": ["Casalux", "Hearth"],
    "Fashion": ["Lumen", "Kite"],
    "Sports": ["Peakr", "Trailz"],
    "Books": ["Paperline"],
}
# Trạng thái đơn + trọng số xuất hiện
ORDER_STATUSES = ["completed", "completed", "completed", "shipped",
                  "pending", "cancelled", "returned"]
PAYMENT_METHODS = ["credit_card", "momo", "bank_transfer", "cod", "paypal"]


def write_csv(filename: str, header: list[str], rows: list[list]) -> None:
    path = RAW_DIR / filename
    # newline="" là bắt buộc trên Windows, nếu không file sẽ có dòng trống
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  [OK] {filename:<20} {len(rows):>6,} dong")


def random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def gen_customers(rng: random.Random) -> list[list]:
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        country, city = rng.choice(CITIES)
        signup = random_date(rng, START_DATE, END_DATE - timedelta(days=30))

        # Cố ý làm bẩn: 15% email viết HOA, 10% có khoảng trắng thừa
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        if rng.random() < 0.15:
            email = email.upper()
        if rng.random() < 0.10:
            email = f"  {email} "

        # 5% khách thiếu thông tin thành phố
        city_value = "" if rng.random() < 0.05 else city

        rows.append([
            f"C{i:05d}", first, last, email, country, city_value,
            signup.isoformat(),
            "true" if rng.random() < 0.85 else "false",
        ])
    return rows


def gen_products(rng: random.Random) -> list[list]:
    rows = []
    pid = 0
    # Chia đều N_PRODUCTS cho tổng số brand, tối thiểu 1 sản phẩm/brand
    n_brands = sum(len(b) for b in CATEGORIES.values())
    per_brand = max(1, N_PRODUCTS // n_brands)

    for category, brands in CATEGORIES.items():
        for brand in brands:
            for n in range(1, per_brand + 1):
                pid += 1
                cost = round(rng.uniform(3, 400), 2)
                # Giá bán = giá vốn cộng biên 20-80%
                price = round(cost * rng.uniform(1.2, 1.8), 2)
                rows.append([
                    f"P{pid:04d}",
                    f"{brand} {category.split()[0]} {n}",
                    category,
                    brand,
                    f"{price:.2f}",
                    f"{cost:.2f}",
                    "true" if rng.random() < 0.08 else "false",
                ])
    return rows


def gen_orders_and_items(
    rng: random.Random, customer_ids: list[str], products: list[list]
) -> tuple[list[list], list[list]]:
    orders, items = [], []
    item_id = 0

    for i in range(1, N_ORDERS + 1):
        order_id = f"O{i:06d}"
        customer_id = rng.choice(customer_ids)
        order_dt = random_date(rng, START_DATE, END_DATE)
        status = rng.choice(ORDER_STATUSES)

        orders.append([
            order_id, customer_id, order_dt.isoformat(), status,
            rng.choice(PAYMENT_METHODS),
        ])

        # Mỗi đơn có 1-5 dòng hàng, không trùng sản phẩm
        for product in rng.sample(products, rng.randint(1, 5)):
            item_id += 1
            list_price = float(product[4])
            # Giá bán thực tế dao động nhẹ quanh giá niêm yết
            sold_price = round(list_price * rng.uniform(0.95, 1.05), 2)
            discount = rng.choice([0, 0, 0, 5, 10, 15, 25])
            items.append([
                f"OI{item_id:07d}", order_id, product[0],
                rng.randint(1, 4), f"{sold_price:.2f}", discount,
            ])

    return orders, items


def main() -> None:
    rng = random.Random(RANDOM_SEED)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Sinh du lieu mau vao: {RAW_DIR}")

    customers = gen_customers(rng)
    products = gen_products(rng)
    orders, items = gen_orders_and_items(
        rng, [c[0] for c in customers], products
    )

    write_csv(
        "customers.csv",
        ["customer_id", "first_name", "last_name", "email", "country",
         "city", "signup_date", "is_active"],
        customers,
    )
    write_csv(
        "products.csv",
        ["product_id", "product_name", "category", "brand", "unit_price",
         "cost_price", "is_discontinued"],
        products,
    )
    write_csv(
        "orders.csv",
        ["order_id", "customer_id", "order_date", "order_status",
         "payment_method"],
        orders,
    )
    write_csv(
        "order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity",
         "unit_price", "discount_pct"],
        items,
    )

    print("\nXong. Buoc tiep theo: python scripts/load_to_bigquery.py")


if __name__ == "__main__":
    main()

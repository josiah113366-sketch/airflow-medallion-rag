import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "ecommerce"),
    user=os.getenv("DB_USER", "ecommerce"),
    password=os.getenv("DB_PASSWORD", "ecommerce"),
)

queries = [
    ("product_metrics", "SELECT process_date, product_id, order_count, refund_count, refund_rate, avg_rating FROM product_metrics ORDER BY refund_rate DESC"),
    ("daily_kpi", "SELECT * FROM daily_kpi ORDER BY process_date DESC"),
    ("knowledge_documents", "SELECT source_type, count(*) FROM knowledge_documents GROUP BY source_type ORDER BY source_type"),
    ("document_chunks", "SELECT source_type, count(*) FROM document_chunks GROUP BY source_type ORDER BY source_type"),
]

with conn.cursor() as cur:
    for title, sql in queries:
        print(f"\n=== {title} ===")
        cur.execute(sql)
        for row in cur.fetchall():
            print(row)

conn.close()

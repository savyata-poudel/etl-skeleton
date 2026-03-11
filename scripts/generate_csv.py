from __future__ import annotations
import csv, random, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from faker import Faker
from etl.config import load_settings

fake = Faker()
random.seed(42)
DEPARTMENTS = ["Engineering","Marketing","Sales","HR","Finance","Operations","Product","Design","Legal","Support"]
FIELDNAMES = ["id","first_name","last_name","email","department","hire_date","salary","is_active"]

def random_hire_date() -> str:
    start = datetime(2010,1,1,tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)
    return (start + timedelta(days=random.randint(0,(end-start).days))).isoformat()

def generate(n: int) -> list[dict]:
    rows = []
    for i in range(1, n+1):
        rows.append({"id":i,"first_name":fake.first_name().strip(),"last_name":fake.last_name().strip(),
                     "email":fake.email(),"department":random.choice(DEPARTMENTS),
                     "hire_date":random_hire_date(),"salary":round(random.uniform(35000,180000),2),
                     "is_active":random.choice([True,False])})
    bad_n = max(2, n//20)
    for _ in range(bad_n):
        rows.append({"id":"","first_name":fake.first_name(),"last_name":fake.last_name(),
                     "email":fake.email(),"department":random.choice(DEPARTMENTS),
                     "hire_date":random_hire_date(),"salary":round(random.uniform(35000,180000),2),"is_active":True})
    for j in range(bad_n):
        rows.append({"id":n+bad_n+j+1,"first_name":"   ","last_name":fake.last_name(),
                     "email":fake.email(),"department":random.choice(DEPARTMENTS),
                     "hire_date":random_hire_date(),"salary":round(random.uniform(35000,180000),2),"is_active":True})
    random.shuffle(rows)
    return rows

def main():
    cfg = load_settings()
    out = Path(cfg["sources"]["file"]["path"])
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = generate(50)
    with open(out,"w",newline="",encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {len(rows)} rows -> {out}")

if __name__ == "__main__":
    main()

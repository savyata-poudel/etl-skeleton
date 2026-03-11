from __future__ import annotations
import sys
from pathlib import Path
from datetime import timezone
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from etl.transform.core import RunMetrics, normalize_api_users, normalize_file_users, normalize_db_users

def m(): return RunMetrics()

def test_api_valid():
    raw = [{"id":1,"email":" Alice@Example.COM ","first_name":" Alice ","last_name":"Smith","avatar":None}]
    metrics = m(); result = list(normalize_api_users(iter(raw), metrics))
    assert len(result)==1 and result[0].email=="alice@example.com" and result[0].first_name=="Alice"

def test_api_missing_id_rejected():
    raw = [{"id":None,"email":"x@y.com","first_name":"Jane","last_name":"Doe"}]
    metrics = m(); result = list(normalize_api_users(iter(raw), metrics))
    assert result==[] and metrics.rejected_count==1

def test_api_empty_name_rejected():
    raw = [{"id":5,"email":"x@y.com","first_name":"  ","last_name":"Doe"}]
    metrics = m(); result = list(normalize_api_users(iter(raw), metrics))
    assert result==[] and metrics.rejected_count==1

def test_file_valid():
    raw = [{"id":"10","first_name":" John ","last_name":"Smith","email":"JOHN@EX.COM",
            "department":"Eng","hire_date":"2021-03-15","salary":"75000","is_active":"true"}]
    metrics = m(); result = list(normalize_file_users(iter(raw), metrics))
    assert result[0].id==10 and result[0].email=="john@ex.com" and result[0].hire_date.tzinfo==timezone.utc

def test_file_missing_id_rejected():
    raw = [{"id":"","first_name":"Jane","last_name":"S","email":"j@s.com","department":"","hire_date":"","salary":"","is_active":"true"}]
    metrics = m(); assert list(normalize_file_users(iter(raw), metrics))==[] and metrics.rejected_count==1

def test_file_empty_name_rejected():
    raw = [{"id":"9","first_name":"   ","last_name":"S","email":"j@s.com","department":"","hire_date":"","salary":"","is_active":"true"}]
    metrics = m(); assert list(normalize_file_users(iter(raw), metrics))==[] and metrics.rejected_count==1

def test_db_valid():
    from datetime import datetime
    raw = [{"id":42,"first_name":"  Alice  ","last_name":"Wonder","email":"ALICE@EXAMPLE.COM",
            "department":"HR","hire_date":datetime(2019,7,1),"salary":95000.0,"is_active":True}]
    metrics = m(); result = list(normalize_db_users(iter(raw), metrics))
    assert result[0].email=="alice@example.com" and result[0].first_name=="Alice"

def test_db_missing_id_rejected():
    raw = [{"id":None,"first_name":"Alice","last_name":"W","email":"a@b.com","department":None,"hire_date":None,"salary":None,"is_active":True}]
    metrics = m(); assert list(normalize_db_users(iter(raw), metrics))==[] and metrics.rejected_count==1

def test_metrics_summary():
    metrics = m(); metrics.extracted_count=10; metrics.loaded_count=8; metrics.rejected_count=2
    assert metrics.summary()=={"extracted_count":10,"loaded_count":8,"rejected_count":2}

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import io
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_preview_endpoint():
    # Use a small sample of the real CSV for testing
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path), f"Test CSV not found: {csv_path}"
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/preview?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"
    assert isinstance(data["columns"], list), f"Columns is not a list: {data['columns']}"
    assert isinstance(data["data"], list), f"Data is not a list: {data['data']}"

def test_impute_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/impute?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"method": "mean", "columns": json.dumps(["ID"]), "value": ""}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"

def test_encode_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/encode?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"method": "onehot", "columns": json.dumps(["Primary Type"])}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"

def test_scale_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/scale?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"method": "minmax", "columns": json.dumps(["ID"])}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"

def test_drop_columns_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/drop_columns?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"columns": json.dumps(["ID"])}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"
    assert "ID" not in data["columns"], f"Column 'ID' was not dropped: {data['columns']}"

def test_filter_rows_endpoint():
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    # Use a value from the first row for a deterministic test
    import pandas as pd
    df = pd.read_csv(csv_path, nrows=1)
    test_col = df.columns[0]
    test_val = df.iloc[0, 0]
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/filter_rows?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"column": test_col, "value": str(test_val)}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    assert len(data["data"]) <= 3, f"Returned too many rows: {len(data['data'])}"
    # All returned rows should have the filtered value in the specified column
    col_idx = data["columns"].index(test_col)
    for row in data["data"]:
        assert str(row[col_idx]) == str(test_val), f"Row does not match filter: {row}"

def test_rename_columns_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    # Use the first column and rename it
    import pandas as pd
    df = pd.read_csv(csv_path, nrows=1)
    old_col = df.columns[0]
    new_col = old_col + "_renamed"
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/rename_columns?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"rename_map": json.dumps({old_col: new_col})}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert new_col in data["columns"], f"Renamed column not found: {data['columns']}"
    assert old_col not in data["columns"], f"Old column still present: {data['columns']}"

def test_change_dtypes_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    import pandas as pd
    df = pd.read_csv(csv_path, nrows=1)
    # Try to convert the first column to string (should always succeed)
    col = df.columns[0]
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/change_dtypes?rows=3",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"dtype_map": json.dumps({col: "str"})}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert col in data["columns"], f"Column missing after dtype change: {data['columns']}"
    # All values should be strings
    col_idx = data["columns"].index(col)
    for row in data["data"]:
        assert isinstance(row[col_idx], str) or row[col_idx] is None, f"Value is not string: {row[col_idx]}"

def test_drop_duplicates_endpoint():
    import json
    csv_path = os.path.join(os.path.dirname(__file__), '../../chicago_crime_data.csv')
    assert os.path.exists(csv_path)
    # No subset: drop all duplicate rows
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/drop_duplicates?rows=10",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"subset": ""}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"
    # Subset: drop duplicates based on the first column only
    import pandas as pd
    df = pd.read_csv(csv_path, nrows=1)
    col = df.columns[0]
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/drop_duplicates?rows=10",
            files={"file": ("chicago_crime_data.csv", f, "text/csv")},
            data={"subset": json.dumps([col])}
        )
    assert response.status_code == 200, f"Status code: {response.status_code}, Response: {response.text}"
    data = response.json()
    assert "columns" in data, f"Response missing columns: {data}"
    assert "data" in data, f"Response missing data: {data}"

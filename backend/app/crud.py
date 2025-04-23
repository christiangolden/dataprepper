import pandas as pd
from typing import Tuple, List, Any, Dict
from io import TextIOBase, BufferedReader
import uuid
import hashlib
import time

dropped_columns_cache: Dict[str, Dict[str, list]] = {}
# Session history: session_id -> list of DataFrame CSV strings (stack)
session_history = {}

# Helper to serialize DataFrame to CSV string
def df_to_csv_str(df):
    from io import StringIO
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()

# Helper to load DataFrame from CSV string
def df_from_csv_str(csv_str):
    from io import StringIO
    return pd.read_csv(StringIO(csv_str))

def preview_csv(file: BufferedReader, rows: int) -> Tuple[List[str], List[List[Any]]]:
    """Read first `rows` lines from CSV file-like and return columns and row data."""
    # pandas can read file-like objects directly
    df = pd.read_csv(file, nrows=rows)
    columns = df.columns.tolist()
    data = df.values.tolist()
    return columns, data

def impute_missing(file, columns, method, value=None, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    for col in columns:
        if method == 'mean':
            df[col] = df[col].fillna(df[col].mean())
        elif method == 'median':
            df[col] = df[col].fillna(df[col].median())
        elif method == 'mode':
            df[col] = df[col].fillna(df[col].mode()[0])
        elif method == 'constant':
            df[col] = df[col].fillna(value)
        else:
            raise ValueError(f"Unknown imputation method: {method}")
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def encode_categorical(file, columns, method, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    if method == 'onehot':
        df = pd.get_dummies(df, columns=columns)
    elif method == 'ordinal':
        for col in columns:
            df[col] = df[col].astype('category').cat.codes
    else:
        raise ValueError(f"Unknown encoding method: {method}")
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def scale_numeric(file, columns, method, rows=5):
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import MinMaxScaler, StandardScaler
    df = pd.read_csv(file)
    scaler = MinMaxScaler() if method == 'minmax' else StandardScaler()
    df[columns] = scaler.fit_transform(df[columns])
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def drop_columns(file, columns, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    df = df.drop(columns=columns)
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def drop_columns_with_cache(file, columns, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    dropped = {col: df[col].tolist() for col in columns if col in df.columns}
    df = df.drop(columns=columns)
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    op_id = str(uuid.uuid4())
    dropped_columns_cache[op_id] = dropped
    return preview.columns.tolist(), preview.values.tolist(), op_id

def restore_dropped_columns(file, op_id, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    dropped = dropped_columns_cache.get(op_id)
    if not dropped:
        raise ValueError("No dropped columns found for this operation ID.")
    for col, data in dropped.items():
        # Restore only if lengths match
        if len(data) == len(df):
            df[col] = data
        else:
            raise ValueError(f"Cannot restore column '{col}': row count mismatch.")
    for col in dropped:
        if col not in df.columns:
            df[col] = dropped[col]
    # Convert NaN/inf/-inf to None for JSON serialization
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def filter_rows(file, column, value=None, min_value=None, max_value=None, regex=None, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    if value is not None:
        df = df[df[column] == value]
    if min_value is not None:
        df = df[df[column] >= min_value]
    if max_value is not None:
        df = df[df[column] <= max_value]
    if regex is not None:
        df = df[df[column].astype(str).str.contains(regex, na=False)]
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def rename_columns(file, rename_map, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    df = df.rename(columns=rename_map)
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def change_dtypes(file, dtype_map, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    for col, dtype in dtype_map.items():
        if dtype == 'datetime':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(dtype, errors='ignore')
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def drop_duplicates(file, subset=None, rows=5):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(file)
    if subset:
        df = df.drop_duplicates(subset=subset)
    else:
        df = df.drop_duplicates()
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    return preview.columns.tolist(), preview.values.tolist()

def generate_session_id(file):
    file.seek(0)
    content = file.read(1024 * 1024)
    file.seek(0)
    h = hashlib.sha256(content).hexdigest()
    return f"{h}_{int(time.time())}"

# On session creation, store the initial file state
def create_session(file):
    df = pd.read_csv(file)
    file.seek(0)
    session_id = generate_session_id(file)
    session_history[session_id] = [df_to_csv_str(df)]
    return session_id

# Apply transformation to the latest DataFrame in history, push new state
def apply_transformation(file, session_id, action, columns, params, rows=5):
    import numpy as np
    stack = session_history.setdefault(session_id, [])
    if not stack:
        # If stack is empty, initialize from file
        df = pd.read_csv(file)
    else:
        df = df_from_csv_str(stack[-1])
    # ...existing transformation logic...
    if action == 'drop':
        df = df.drop(columns=columns)
    elif action == 'impute':
        method = params.get('method', 'mean')
        value = params.get('value', None)
        for col in columns:
            if method == 'mean':
                df[col] = df[col].fillna(df[col].mean())
            elif method == 'median':
                df[col] = df[col].fillna(df[col].median())
            elif method == 'mode':
                df[col] = df[col].fillna(df[col].mode()[0])
            elif method == 'constant':
                df[col] = df[col].fillna(value)
            else:
                raise ValueError(f"Unknown imputation method: {method}")
    # TODO: Add support for encode, scale, etc.
    else:
        raise ValueError(f"Unsupported action for history: {action}")
    df = df.replace([np.nan, np.inf, -np.inf], None)
    # Push new state to stack
    stack.append(df_to_csv_str(df))
    preview = df.head(rows)
    can_undo = len(stack) > 0
    return preview.columns.tolist(), preview.values.tolist(), can_undo

# Undo: pop the last state, return the previous one
def undo_last_transformation(file, session_id, rows=5):
    import numpy as np
    stack = session_history.get(session_id, [])
    if not stack or len(stack) == 0:
        raise ValueError("No history to undo.")
    stack.pop()  # Remove last state
    if len(stack):
        df = df_from_csv_str(stack[len(stack)-1])
    else:
        df = pd.read_csv(file)
    df = df.replace([np.nan, np.inf, -np.inf], None)
    preview = df.head(rows)
    can_undo = len(stack) > 0
    return preview.columns.tolist(), preview.values.tolist(), can_undo

def get_column_stats(file, session_id=None):
    import pandas as pd
    import numpy as np
    df = None
    if session_id is not None and session_id in session_history and session_history[session_id]:
        df = df_from_csv_str(session_history[session_id][-1])
    else:
        df = pd.read_csv(file)
    stats = {}
    n_rows = len(df)
    for col in df.columns:
        col_data = df[col]
        col_stats = {
            'count': int(col_data.count()),
            'missing_pct': float(col_data.isnull().mean() * 100),
            'unique': int(col_data.nunique()),
        }
        # Data issue scores
        data_issues = {}
        # Recommendations
        recommendations = []
        # Missingness
        missingness = float(col_data.isnull().mean())
        data_issues['missing'] = missingness  # 0-1
        if missingness > 0.5:
            recommendations.append('Consider dropping this column due to excessive missing data.')
        elif 0.1 < missingness <= 0.5:
            recommendations.append('Consider imputing missing values.')
        # Constant value
        if n_rows > 0:
            value_counts = col_data.value_counts(dropna=False)
            most_common_pct = float(value_counts.iloc[0] / n_rows) if not value_counts.empty else 0.0
        else:
            most_common_pct = 0.0
        data_issues['constant'] = most_common_pct  # 0-1
        if most_common_pct > 0.95:
            recommendations.append('Consider dropping this column as it is nearly constant.')
        # High cardinality
        cardinality = float(col_data.nunique() / n_rows) if n_rows > 0 else 0.0
        data_issues['high_cardinality'] = cardinality  # 0-1
        if cardinality > 0.8:
            recommendations.append('Consider dropping or encoding this column due to high cardinality.')
        # Outlier risk (for numerics)
        if pd.api.types.is_numeric_dtype(col_data):
            col_stats.update({
                'mean': float(col_data.mean()) if col_data.count() else None,
                'median': float(col_data.median()) if col_data.count() else None,
                'std': float(col_data.std()) if col_data.count() else None,
                'min': float(col_data.min()) if col_data.count() else None,
                'max': float(col_data.max()) if col_data.count() else None,
            })
            # Outlier risk: % of values > 3 std from mean
            if col_data.count() > 0 and col_data.std() is not None and col_data.std() > 0:
                outlier_mask = (np.abs(col_data - col_data.mean()) > 3 * col_data.std())
                outlier_risk = float(outlier_mask.sum() / col_data.count())
            else:
                outlier_risk = 0.0
            data_issues['outlier'] = outlier_risk  # 0-1
            if outlier_risk > 0.1:
                recommendations.append('Consider scaling or transforming this column due to high outlier risk.')
            # Add histogram for numeric columns
            try:
                clean_col = col_data.dropna().astype(float)
                if len(clean_col) > 0:
                    hist_counts, hist_edges = np.histogram(clean_col, bins=20)
                    col_stats['histogram'] = {
                        'bin_edges': hist_edges.tolist(),
                        'counts': hist_counts.tolist()
                    }
                else:
                    col_stats['histogram'] = {'bin_edges': [], 'counts': []}
            except Exception:
                col_stats['histogram'] = {'bin_edges': [], 'counts': []}
        else:
            top = col_data.mode().iloc[0] if not col_data.mode().empty else None
            freq = int((col_data == top).sum()) if top is not None else 0
            col_stats.update({
                'top': str(top) if top is not None else None,
                'freq': freq
            })
            data_issues['outlier'] = None  # Not applicable
            # Add value counts for categorical columns (top 20)
            try:
                value_counts = col_data.value_counts(dropna=False).head(20)
                col_stats['value_counts'] = [
                    {'value': str(idx), 'count': int(cnt)} for idx, cnt in value_counts.items()
                ]
            except Exception:
                col_stats['value_counts'] = []
        col_stats['data_issues'] = data_issues
        col_stats['recommendations'] = recommendations
        stats[col] = col_stats
    return stats
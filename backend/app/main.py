import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Form
from fastapi.middleware.cors import CORSMiddleware
from .crud import preview_csv, impute_missing, encode_categorical, scale_numeric, drop_columns, filter_rows, rename_columns, change_dtypes, drop_duplicates, drop_columns_with_cache, restore_dropped_columns, generate_session_id, apply_transformation, undo_last_transformation, get_column_stats
from .models import PreviewResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dataprepper")

app = FastAPI(title="DataPrepper API")

# Allow local frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/preview", response_model=PreviewResponse)
async def preview(file: UploadFile = File(...), rows: int = 5):
    logger.info(f"/preview called with file={file.filename}, rows={rows}")
    try:
        columns, data = preview_csv(file.file, rows)
        logger.info(f"/preview success: columns={columns}")
        return PreviewResponse(columns=columns, data=data)
    except Exception as e:
        logger.error(f"/preview error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/impute", response_model=PreviewResponse)
async def impute(
    file: UploadFile = File(...),
    method: str = Form(...),
    columns: str = Form(...),
    value: str = Form(None),
    rows: int = 5
):
    logger.info(f"/impute called with file={file.filename}, method={method}, columns={columns}, value={value}, rows={rows}")
    try:
        import json
        columns_list = json.loads(columns) if columns.startswith('[') else [columns]
        columns, data = impute_missing(file.file, columns_list, method, value, rows)
        logger.info(f"/impute success: columns={columns}")
        return PreviewResponse(columns=columns, data=data)
    except Exception as e:
        logger.error(f"/impute error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/encode", response_model=PreviewResponse)
async def encode(
    file: UploadFile = File(...),
    method: str = Form(...),
    columns: str = Form(...),
    rows: int = 5
):
    logger.info(f"/encode called with file={file.filename}, method={method}, columns={columns}, rows={rows}")
    try:
        import json
        columns_list = json.loads(columns) if columns.startswith('[') else [columns]
        columns, data = encode_categorical(file.file, columns_list, method, rows)
        logger.info(f"/encode success: columns={columns}")
        return PreviewResponse(columns=columns, data=data)
    except Exception as e:
        logger.error(f"/encode error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/scale", response_model=PreviewResponse)
async def scale(
    file: UploadFile = File(...),
    method: str = Form(...),
    columns: str = Form(...),
    rows: int = 5
):
    logger.info(f"/scale called with file={file.filename}, method={method}, columns={columns}, rows={rows}")
    try:
        import json
        columns_list = json.loads(columns) if columns.startswith('[') else [columns]
        columns, data = scale_numeric(file.file, columns_list, method, rows)
        logger.info(f"/scale success: columns={columns}")
        return PreviewResponse(columns=columns, data=data)
    except Exception as e:
        logger.error(f"/scale error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/drop_columns", response_model=PreviewResponse)
async def drop_columns_endpoint(
    file: UploadFile = File(...),
    columns: str = Form(...),
    rows: int = 5
):
    logger.info(f"/drop_columns called with file={file.filename}, columns={columns}, rows={rows}")
    try:
        import json
        columns_list = json.loads(columns) if columns.startswith('[') else [columns]
        cols, data = drop_columns(file.file, columns_list, rows)
        logger.info(f"/drop_columns success: columns={cols}")
        return PreviewResponse(columns=cols, data=data)
    except Exception as e:
        logger.error(f"/drop_columns error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/filter_rows", response_model=PreviewResponse)
async def filter_rows_endpoint(
    file: UploadFile = File(...),
    column: str = Form(...),
    value: str = Form(None),
    min_value: str = Form(None),
    max_value: str = Form(None),
    regex: str = Form(None),
    rows: int = 5
):
    logger.info(f"/filter_rows called with file={file.filename}, column={column}, value={value}, min_value={min_value}, max_value={max_value}, regex={regex}, rows={rows}")
    try:
        cols, data = filter_rows(file.file, column, value, min_value, max_value, regex, rows)
        logger.info(f"/filter_rows success: columns={cols}")
        return PreviewResponse(columns=cols, data=data)
    except Exception as e:
        logger.error(f"/filter_rows error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/rename_columns", response_model=PreviewResponse)
async def rename_columns_endpoint(
    file: UploadFile = File(...),
    rename_map: str = Form(...),
    rows: int = 5
):
    logger.info(f"/rename_columns called with file={file.filename}, rename_map={rename_map}, rows={rows}")
    try:
        import json
        rename_map_dict = json.loads(rename_map)
        cols, data = rename_columns(file.file, rename_map_dict, rows)
        logger.info(f"/rename_columns success: columns={cols}")
        return PreviewResponse(columns=cols, data=data)
    except Exception as e:
        logger.error(f"/rename_columns error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/change_dtypes", response_model=PreviewResponse)
async def change_dtypes_endpoint(
    file: UploadFile = File(...),
    dtype_map: str = Form(...),
    rows: int = 5
):
    logger.info(f"/change_dtypes called with file={file.filename}, dtype_map={dtype_map}, rows={rows}")
    try:
        import json
        dtype_map_dict = json.loads(dtype_map)
        cols, data = change_dtypes(file.file, dtype_map_dict, rows)
        logger.info(f"/change_dtypes success: columns={cols}")
        return PreviewResponse(columns=cols, data=data)
    except Exception as e:
        logger.error(f"/change_dtypes error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/drop_duplicates", response_model=PreviewResponse)
async def drop_duplicates_endpoint(
    file: UploadFile = File(...),
    subset: str = Form(None),
    rows: int = 5
):
    logger.info(f"/drop_duplicates called with file={file.filename}, subset={subset}, rows={rows}")
    try:
        import json
        subset_list = json.loads(subset) if subset else None
        cols, data = drop_duplicates(file.file, subset_list, rows)
        logger.info(f"/drop_duplicates success: columns={cols}")
        return PreviewResponse(columns=cols, data=data)
    except Exception as e:
        logger.error(f"/drop_duplicates error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/drop_columns_with_cache")
async def drop_columns_with_cache_endpoint(
    file: UploadFile = File(...),
    columns: str = Form(...),
    rows: int = 5
):
    logger.info(f"/drop_columns_with_cache called with file={file.filename}, columns={columns}, rows={rows}")
    try:
        import json
        columns_list = json.loads(columns) if columns.startswith('[') else [columns]
        cols, data, op_id = drop_columns_with_cache(file.file, columns_list, rows)
        logger.info(f"/drop_columns_with_cache success: columns={cols}, op_id={op_id}")
        return {"columns": cols, "data": data, "operation_id": op_id}
    except Exception as e:
        logger.error(f"/drop_columns_with_cache error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/restore_dropped_columns")
async def restore_dropped_columns_endpoint(
    file: UploadFile = File(...),
    operation_id: str = Form(...),
    rows: int = 5
):
    logger.info(f"/restore_dropped_columns called with file={file.filename}, operation_id={operation_id}, rows={rows}")
    try:
        cols, data = restore_dropped_columns(file.file, operation_id, rows)
        logger.info(f"/restore_dropped_columns success: columns={cols}")
        return {"columns": cols, "data": data}
    except Exception as e:
        logger.error(f"/restore_dropped_columns error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/create_session")
async def create_session(file: UploadFile = File(...)):
    session_id = generate_session_id(file.file)
    return {"session_id": session_id}

@app.post("/apply_transformation")
async def apply_transformation_endpoint(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    action: str = Form(...),
    columns: str = Form(...),
    params: str = Form('{}'),
    rows: int = 5
):
    import json
    columns_list = json.loads(columns) if columns.startswith('[') else [columns]
    params_dict = json.loads(params) if params else {}
    cols, data, can_undo = apply_transformation(file.file, session_id, action, columns_list, params_dict, rows)
    return {"columns": cols, "data": data, "can_undo": can_undo}

@app.post("/undo")
async def undo_endpoint(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    rows: int = 5
):
    cols, data, can_undo = undo_last_transformation(file.file, session_id, rows)
    return {"columns": cols, "data": data, "can_undo": can_undo}

@app.post("/column_stats")
async def column_stats_endpoint(
    file: UploadFile = File(...),
    session_id: str = Form(None)
):
    stats = get_column_stats(file.file, session_id=session_id)
    return {"stats": stats}

import React, { useState } from 'react';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { Button, Box, Typography, CircularProgress } from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import { FormControl, InputLabel, Select, MenuItem, OutlinedInput, Checkbox, ListItemText, TextField } from '@mui/material';
import { saveAs } from 'file-saver';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend, PieChart, Pie, Cell, LineChart, Line, ReferenceLine } from 'recharts';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import { ResizableBox } from 'react-resizable';
import 'react-resizable/css/styles.css';

const API_URL = 'http://127.0.0.1:8000/preview';

const DataUploadPreview: React.FC = () => {
  const [file, setFile] = useState<File | null>(null);
  const [columns, setColumns] = useState<GridColDef[]>([]);
  const [rows, setRows] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 });
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [action, setAction] = useState<string>('');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [imputeMethod, setImputeMethod] = useState<string>('mean');
  const [imputeConstant, setImputeConstant] = useState<string>('');
  const [canUndo, setCanUndo] = useState(false);
  const [undoCount, setUndoCount] = useState<number>(0);
  const [colStats, setColStats] = useState<any>({});
  const [statsColumns, setStatsColumns] = useState<string[]>([]);
  const [colVizTypes, setColVizTypes] = useState<{ [col: string]: string }>({});
  const [dismissedRecs, setDismissedRecs] = useState<{ [col: string]: Set<string> }>({});
  const actionOptions = [
    { value: 'impute', label: 'Impute Missing Values' },
    { value: 'encode', label: 'Encode Categorical' },
    { value: 'scale', label: 'Scale Numeric' },
    { value: 'drop', label: 'Drop Columns' },
    // Add more actions as needed
  ];
  const vizOptions = {
    numeric: [
      { value: 'bar', label: 'Bar (Min/Mean/Median/Max)' },
      { value: 'hist', label: 'Histogram' },
      { value: 'box', label: 'Boxplot' },
      { value: 'violin', label: 'Violin Plot' },
      { value: 'missing', label: 'Missingness Bar' },
    ],
    categorical: [
      { value: 'bar', label: 'Bar (Top/Other)' },
      { value: 'pie', label: 'Pie (Top/Other)' },
      { value: 'fullbar', label: 'Full Bar (All Values)' },
      { value: 'missing', label: 'Missingness Bar' },
    ],
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      const newFile = e.target.files[0];
      setFile(newFile);
      setColumns([]);
      setRows([]);
      setError(null);
      setSessionId(null);
      // Create a session for this file
      const formData = new FormData();
      formData.append('file', newFile);
      try {
        const response = await fetch('http://127.0.0.1:8000/create_session', {
          method: 'POST',
          body: formData,
        });
        if (!response.ok) throw new Error(await response.text());
        const data = await response.json();
        setSessionId(data.session_id);
        setCanUndo(false);
      } catch (err: any) {
        setError('Failed to create session: ' + (err.message || err));
      }
    }
  };

  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('rows', '10');
    try {
      const response = await fetch(API_URL + '?rows=10', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      // Convert columns to DataGrid format
      const gridCols = data.columns.map((col: string, idx: number) => ({
        field: col,
        headerName: col,
        width: 150,
      }));
      // Add id field for DataGrid
      const gridRows = data.data.map((row: any[], idx: number) => {
        const rowObj: any = { id: idx };
        data.columns.forEach((col: string, i: number) => {
          rowObj[col] = row[i];
        });
        return rowObj;
      });
      setColumns(gridCols);
      setRows(gridRows);
      await fetchColumnStats(file, true); // resetOnMissing true for file upload
    } catch (err: any) {
      setError(err.message || 'Upload failed');
    } finally {
      setLoading(false);
    }
  };

  const handleRunPreprocessing = async () => {
    if (!file || selectedColumns.length === 0 || !action || !sessionId) return;
    setLoading(true);
    setError(null);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', sessionId);
    formData.append('action', action);
    formData.append('columns', JSON.stringify(selectedColumns));
    let params: any = {};
    if (action === 'impute') {
      params.method = imputeMethod;
      if (imputeMethod === 'constant') {
        params.value = imputeConstant;
      }
    }
    formData.append('params', JSON.stringify(params));
    try {
      const response = await fetch('http://127.0.0.1:8000/apply_transformation?rows=10', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      await fetchColumnStats(file, false);
      const gridCols = data.columns.map((col: string, idx: number) => ({
        field: col,
        headerName: col,
        width: 150,
      }));
      const gridRows = data.data.map((row: any[], idx: number) => {
        const rowObj: any = { id: idx };
        data.columns.forEach((col: string, i: number) => {
          rowObj[col] = row[i];
        });
        return rowObj;
      });
      setColumns(gridCols);
      setRows(gridRows);
      setCanUndo(true);
      setUndoCount(undoCount + 1);
      setSelectedColumns([]); // Reset column selection after action
    } catch (err: any) {
      setError(err.message || 'Preprocessing failed');
    } finally {
      setLoading(false);
    }
  };

  const handleUndo = async () => {
    if (!file || !sessionId) return;
    setLoading(true);
    setError(null);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', sessionId);
    try {
      const response = await fetch('http://127.0.0.1:8000/undo?rows=10', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      const gridCols = data.columns.map((col: string, idx: number) => ({
        field: col,
        headerName: col,
        width: 150,
      }));
      const gridRows = data.data.map((row: any[], idx: number) => {
        const rowObj: any = { id: idx };
        data.columns.forEach((col: string, i: number) => {
          rowObj[col] = row[i];
        });
        return rowObj;
      });
      setColumns(gridCols);
      setRows(gridRows);
      setUndoCount(undoCount - 1);
      setCanUndo(!!data.can_undo);
      await fetchColumnStats(file, false); // preserve customizations
    } catch (err: any) {
      setError(err.message || 'Undo failed');
      setCanUndo(false);
    } finally {
      setLoading(false);
    }
  };

  // Export current data as CSV
  const handleExportCSV = () => {
    if (columns.length === 0 || rows.length === 0) return;
    const csvRows = [];
    // Header
    csvRows.push(columns.map(col => col.headerName).join(','));
    // Data
    rows.forEach(row => {
      csvRows.push(columns.map(col => JSON.stringify(row[col.field] ?? '')).join(','));
    });
    const csvString = csvRows.join('\n');
    const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
    saveAs(blob, 'processed_data.csv');
  };

  // Helper to fetch and set column stats
  const fetchColumnStats = async (fileObj: File | null, resetOnMissing = false) => {
    if (!fileObj) return;
    const formData = new FormData();
    formData.append('file', fileObj);
    if (sessionId) {
      formData.append('session_id', sessionId);
    }
    const response = await fetch('http://127.0.0.1:8000/column_stats', {
      method: 'POST',
      body: formData,
    });
    if (response.ok) {
      const data = await response.json();
      console.log('DEBUG /column_stats response:', data);
      setColStats(data.stats);
      const colNames = Object.keys(data.stats);
      setStatsColumns(prev => {
        const filtered = prev.filter(col => colNames.includes(col));
        if (filtered.length > 0) return filtered;
        // Use healthiest columns for default
        return getHealthiestColumns(data.stats, 5);
      });
      setColVizTypes(prev => {
        const filtered: { [col: string]: string } = {};
        for (const col of Object.keys(prev)) {
          if (colNames.includes(col)) filtered[col] = prev[col];
        }
        return filtered;
      });
      // If any column in dismissedRecs is not in colStats, reset dismissedRecs
      setDismissedRecs(prev => {
        const prevCols = Object.keys(prev);
        if (prevCols.some(col => !colNames.includes(col))) {
          return {};
        }
        return prev;
      });
      // Reset dismissedRecs to only current columns/recs
      syncDismissedRecs(data.stats);
    }
  };

  // Helper to get column type
  const getColType = (col: string) => {
    const stat = colStats[col];
    if (!stat) return 'unknown';
    if ('mean' in stat) return 'numeric';
    if ('top' in stat) return 'categorical';
    return 'unknown';
  };

  // Helper to get all value counts for a categorical column
  const getCategoricalCounts = (col: string) => {
    const counts: Record<string, number> = {};
    rows.forEach(r => {
      const v = r[col];
      counts[v == null ? 'null' : String(v)] = (counts[v == null ? 'null' : String(v)] || 0) + 1;
    });
    return Object.entries(counts).map(([name, value]) => ({ name, value }));
  };

  // Helper to render a chart for a column
  const renderColumnChart = (col: string) => {
    const stat = colStats[col];
    if (!stat) return null;
    const colType = getColType(col);
    const vizType = colVizTypes[col] || (colType === 'numeric' ? 'bar' : 'bar');
    // All charts now fill parent (ResponsiveContainer width/height 100%)
    // Numeric: Bar (min/mean/median/max)
    if (colType === 'numeric' && vizType === 'bar') {
      const chartData = [
        { name: 'Min', value: stat.min },
        { name: 'Mean', value: stat.mean },
        { name: 'Median', value: stat.median },
        { name: 'Max', value: stat.max },
      ];
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="value" fill="#1976d2" />
          </BarChart>
        </ResponsiveContainer>
      );
    }
    // Numeric: Histogram (use stat.histogram)
    if (colType === 'numeric' && vizType === 'hist' && stat.histogram) {
      const hist = stat.histogram;
      if (!hist.counts || !hist.bin_edges || hist.counts.length === 0) return <Typography variant="body2">No data for histogram</Typography>;
      const chartData = hist.counts.map((count: number, i: number) => ({
        name: `${hist.bin_edges[i].toFixed(2)}-${hist.bin_edges[i+1].toFixed(2)}`,
        value: count,
      }));
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" interval={0} angle={-30} textAnchor="end" height={60} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="value" fill="#ffa000" />
          </BarChart>
        </ResponsiveContainer>
      );
    }
    // Numeric: Boxplot (show as min, Q1, median, Q3, max) - fallback to summary stats
    if (colType === 'numeric' && vizType === 'box') {
      const min = stat.min;
      const max = stat.max;
      const median = stat.median;
      let q1 = null, q3 = null;
      if (stat.histogram && stat.histogram.counts && stat.histogram.counts.length > 0) {
        const total = stat.histogram.counts.reduce((a: number, b: number) => a + b, 0);
        let cum = 0;
        for (let i = 0; i < stat.histogram.counts.length; ++i) {
          cum += stat.histogram.counts[i];
          if (!q1 && cum >= total * 0.25) q1 = stat.histogram.bin_edges[i];
          if (!q3 && cum >= total * 0.75) q3 = stat.histogram.bin_edges[i];
        }
      }
      const chartData = [
        { name: 'Min', value: min },
        { name: 'Q1', value: q1 ?? min },
        { name: 'Median', value: median },
        { name: 'Q3', value: q3 ?? max },
        { name: 'Max', value: max },
      ];
      return (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="value" stroke="#1976d2" dot />
            <ReferenceLine x="Median" stroke="#d32f2f" label="Median" />
          </LineChart>
        </ResponsiveContainer>
      );
    }
    // Numeric: Violin plot (use histogram as proxy)
    if (colType === 'numeric' && vizType === 'violin' && stat.histogram) {
      const hist = stat.histogram;
      if (!hist.counts || !hist.bin_edges || hist.counts.length === 0) return <Typography variant="body2">No data for violin plot</Typography>;
      const chartData = hist.counts.map((count: number, i: number) => ({
        name: `${hist.bin_edges[i].toFixed(2)}`,
        value: count,
      }));
      return (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <XAxis dataKey="name" interval={0} angle={-30} textAnchor="end" height={60} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="value" stroke="#ffa000" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      );
    }
    // Numeric or Categorical: Missingness bar
    if (vizType === 'missing') {
      const missingPct = stat.missing_pct;
      return (
        <Box sx={{ width: '100%', height: 40, display: 'flex', alignItems: 'center' }}>
          <Box sx={{ width: '80%', mr: 2 }}>
            <BarChart width={180} height={30} data={[{ name: col, missing: missingPct, present: 100 - missingPct }]}> 
              <Bar dataKey="present" stackId="a" fill="#43a047" />
              <Bar dataKey="missing" stackId="a" fill="#d32f2f" />
            </BarChart>
          </Box>
          <Typography variant="body2">{missingPct.toFixed(2)}% missing</Typography>
        </Box>
      );
    }
    // Categorical: Bar (top/other)
    if (colType === 'categorical' && vizType === 'bar') {
      const chartData = [
        { name: stat.top, value: stat.freq },
        { name: 'Other', value: stat.count - stat.freq },
      ];
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="value" fill="#43a047" />
          </BarChart>
        </ResponsiveContainer>
      );
    }
    // Categorical: Pie (top/other)
    if (colType === 'categorical' && vizType === 'pie') {
      const chartData = [
        { name: stat.top, value: stat.freq },
        { name: 'Other', value: stat.count - stat.freq },
      ];
      const COLORS = ['#0088FE', '#FFBB28'];
      return (
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={60} label>
              {chartData.map((entry, idx) => (
                <Cell key={`cell-${idx}`} fill={COLORS[idx % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip />
            <Legend />
          </PieChart>
        </ResponsiveContainer>
      );
    }
    // Categorical: Full bar (all values, use stat.value_counts)
    if (colType === 'categorical' && vizType === 'fullbar' && stat.value_counts) {
      const chartData = stat.value_counts.map((vc: any) => ({ name: vc.value, value: vc.count }));
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" interval={0} angle={-30} textAnchor="end" height={60} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="value" fill="#1976d2" />
          </BarChart>
        </ResponsiveContainer>
      );
    }
    return null;
  };

  // Helper to determine if a column has a data issue (e.g., high missingness)
  const hasDataIssue = (col: string) => {
    const stat = colStats[col];
    if (!stat) return false;
    return stat.missing_pct > 20; // threshold for warning
  };

  // Helper to render a data quality heatmap/table
  const renderDataQualityHeatmap = () => {
    if (!colStats || Object.keys(colStats).length === 0) return null;
    const issueTypes = [
      { key: 'missing', label: 'Missing' },
      { key: 'constant', label: 'Constant' },
      { key: 'high_cardinality', label: 'High Cardinality' },
      { key: 'outlier', label: 'Outlier' },
    ];
    const columnsList = Object.keys(colStats);
    // Helper to color cells based on risk
    const getCellColor = (score: number|null|undefined, issue: string) => {
      if (score == null) return '#eee';
      // Custom thresholds per issue
      if (issue === 'missing') {
        if (score > 0.2) return '#e57373'; // red
        if (score > 0.05) return '#fff176'; // yellow
        return '#81c784'; // green
      }
      if (issue === 'constant') {
        if (score > 0.95) return '#e57373';
        if (score > 0.7) return '#fff176';
        return '#81c784';
      }
      if (issue === 'high_cardinality') {
        if (score > 0.5) return '#e57373';
        if (score > 0.2) return '#fff176';
        return '#81c784';
      }
      if (issue === 'outlier') {
        if (score > 0.1) return '#e57373';
        if (score > 0.03) return '#fff176';
        return '#81c784';
      }
      return '#eee';
    };
    return (
      <Box sx={{ overflowX: 'auto', mb: 3 }}>
        <Typography variant="subtitle2" sx={{ mb: 1 }}>Data Quality Heatmap</Typography>
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              <th style={{ textAlign: 'left', padding: 4, minWidth: 90, background: '#fafafa' }}>Issue</th>
              {columnsList.map(col => (
                <th key={col} style={{ textAlign: 'center', padding: 4, minWidth: 90, background: '#fafafa' }}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {issueTypes.map(issue => (
              <tr key={issue.key}>
                <td style={{ fontWeight: 500, padding: 4 }}>{issue.label}</td>
                {columnsList.map(col => {
                  const score = colStats[col]?.data_issues?.[issue.key];
                  let display = score == null ? '-' : (score * 100).toFixed(1) + '%';
                  if (issue.key === 'outlier' && score == null) display = 'N/A';
                  return (
                    <td
                      key={col + issue.key}
                      style={{
                        background: getCellColor(score, issue.key),
                        color: '#222',
                        textAlign: 'center',
                        padding: 4,
                        border: '1px solid #eee',
                        fontWeight: score != null && score > 0.2 ? 600 : 400,
                        fontSize: 14,
                      }}
                      title={display}
                    >
                      {display}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    );
  };

  // Helper to get recommended actions for columns
  const getRecommendations = () => {
    if (!colStats) return [];
    const recs: { col: string; recs: string[] }[] = [];
    Object.entries(colStats).forEach(([col, stat]: any) => {
      if (stat.recommendations && stat.recommendations.length > 0) {
        recs.push({ col, recs: stat.recommendations });
      }
    });
    return recs;
  };

  // Helper to get visible (not dismissed) recommendations
  const getVisibleRecommendations = () => {
    const recs = getRecommendations();
    return recs.flatMap(({ col, recs }) =>
      recs.filter(rec => !(dismissedRecs[col]?.has(rec)))
    );
  };

  // After stats update, reset dismissedRecs to only include columns and recs that still exist
  const syncDismissedRecs = (stats: any) => {
    setDismissedRecs(prev => {
      const newDismissed: { [col: string]: Set<string> } = {};
      for (const col of Object.keys(stats)) {
        // Only keep recs that are still present in recommendations
        const stat = stats[col];
        if (!stat || !stat.recommendations) continue;
        const prevSet = prev[col] || new Set();
        const validRecs = new Set(
          Array.from(prevSet).filter(
            rec => (stat.recommendations || []).includes(rec)
          )
        );
        if (validRecs.size > 0) newDismissed[col] = validRecs;
      }
      return newDismissed;
    });
  };

  // Helper to get healthiest columns for default visualization
  const getHealthiestColumns = (dataStats: any, count: number) => {
    // Exclude columns with strong drop recommendations
    const dropWords = ['drop', 'excessive missing', 'nearly constant', 'high cardinality'];
    const cols = Object.entries(dataStats || {})
      .filter(([col, stat]: any) =>
        !(stat.recommendations || []).some((rec: string) => dropWords.some(word => rec.toLowerCase().includes(word)))
      )
      .map(([col, stat]: any) => {
        // Health score: sum of data_issues (lower is better)
        const issues = stat.data_issues || {};
        let score = 0;
        for (const k of ['missing', 'constant', 'high_cardinality', 'outlier']) {
          const v = issues[k];
          if (typeof v === 'number') score += v;
        }
        return { col, score };
      })
      .sort((a, b) => a.score - b.score)
      .map(x => x.col);
    // Fallback: if not enough, just take first N
    return cols.slice(0, count).length > 0 ? cols.slice(0, count) : Object.keys(dataStats).slice(0, count);
  };

  // One-click action handler
  const handleOneClickAction = async (col: string, rec: string) => {
    if (!file || !sessionId) return;
    let action = '';
    let params: any = {};
    let columns = [col];
    if (rec.toLowerCase().includes('drop')) {
      action = 'drop';
    } else if (rec.toLowerCase().includes('imput')) {
      action = 'impute';
      params.method = 'mean'; // default to mean
    } else if (rec.toLowerCase().includes('encode')) {
      action = 'encode';
      params.method = 'onehot'; // default to onehot
    } else if (rec.toLowerCase().includes('scal') || rec.toLowerCase().includes('transform')) {
      action = 'scale';
      params.method = 'standard'; // default to standard
    } else {
      return;
    }
    setSelectedColumns([col]);
    setAction(action);
    setLoading(true);
    setError(null);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', sessionId);
    formData.append('action', action);
    formData.append('columns', JSON.stringify(columns));
    formData.append('params', JSON.stringify(params));
    try {
      const response = await fetch('http://127.0.0.1:8000/apply_transformation?rows=10', {
        method: 'POST',
        body: formData,
      });
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      // Always await fetchColumnStats before updating columns/rows
      await fetchColumnStats(file, false);
      const gridCols = data.columns.map((col: string, idx: number) => ({
        field: col,
        headerName: col,
        width: 150,
      }));
      const gridRows = data.data.map((row: any[], idx: number) => {
        const rowObj: any = { id: idx };
        data.columns.forEach((col: string, i: number) => {
          rowObj[col] = row[i];
        });
        return rowObj;
      });
      setColumns(gridCols);
      setRows(gridRows);
      setCanUndo(true);
      setUndoCount(undoCount + 1);
      // Mark this recommendation as dismissed
      setDismissedRecs(prev => {
        const newSet = new Set(Array.from(prev[col] || []));
        newSet.add(rec);
        return { ...prev, [col]: newSet };
      });
    } catch (err: any) {
      setError(err.message || 'Preprocessing failed');
    } finally {
      setLoading(false);
    }
  };

  // Helper to render recommendations
  const renderRecommendations = () => {
    const recs = getRecommendations();
    const visibleRecs = recs.flatMap(({ col, recs }) =>
      recs.filter(rec => !(dismissedRecs[col]?.has(rec)))
    );
    if (visibleRecs.length === 0) return null;
    return (
      <Box sx={{ mb: 2, p: 2, background: '#fffde7', border: '1px solid #ffe082', borderRadius: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 1 }}>
          <strong>Preprocessing Recommendations</strong>
        </Typography>
        <ul style={{ margin: 0, paddingLeft: 20 }}>
          {recs.map(({ col, recs }) =>
            recs.filter(rec => !(dismissedRecs[col]?.has(rec))).map((rec, i) => (
              <li key={col + i} style={{ marginBottom: 8 }}>
                <strong>{col}:</strong> {rec}
                <Button
                  variant="outlined"
                  size="small"
                  sx={{ ml: 2, fontSize: 12, py: 0.5, px: 1.5 }}
                  onClick={() => handleOneClickAction(col, rec)}
                  disabled={loading}
                >
                  Apply
                </Button>
              </li>
            ))
          )}
        </ul>
      </Box>
    );
  };

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h5" gutterBottom>Upload CSV and Preview Data</Typography>
      <label htmlFor="upload-csv-file">
        <input
          id="upload-csv-file"
          type="file"
          accept=".csv"
          style={{ display: 'none' }}
          onChange={handleFileChange}
        />
        <Button
          variant="outlined"
          component="span"
          startIcon={<UploadFileIcon />}
          sx={{ mb: 2 }}
        >
          {file ? file.name : 'Select CSV File'}
        </Button>
      </label>
      <Button
        variant="contained"
        color="primary"
        onClick={handleUpload}
        disabled={!file || loading}
        sx={{ ml: 2, mb: 2 }}
      >
        Preview
      </Button>
      {/* Column selector and action menu */}
      {columns.length > 0 && (
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', mb: 2 }}>
          <FormControl sx={{ minWidth: 200 }} size="small">
            <InputLabel id="column-select-label">Columns</InputLabel>
            <Select
              labelId="column-select-label"
              multiple
              value={selectedColumns}
              onChange={e => setSelectedColumns(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
              input={<OutlinedInput label="Columns" />}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {columns.map(col => (
                <MenuItem key={col.field} value={col.field}>
                  <Checkbox checked={selectedColumns.indexOf(col.field) > -1} />
                  <ListItemText primary={col.headerName} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl sx={{ minWidth: 220 }} size="small">
            <InputLabel id="action-select-label">Preprocessing Action</InputLabel>
            <Select
              labelId="action-select-label"
              value={action}
              onChange={e => setAction(e.target.value)}
              label="Preprocessing Action"
            >
              {actionOptions.map(opt => (
                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
              ))}
            </Select>
          </FormControl>
          {action === 'impute' && (
            <FormControl sx={{ minWidth: 180, ml: 2 }} size="small">
              <InputLabel id="impute-method-label">Impute Method</InputLabel>
              <Select
                labelId="impute-method-label"
                value={imputeMethod}
                label="Impute Method"
                onChange={e => setImputeMethod(e.target.value)}
              >
                <MenuItem value="mean">Mean</MenuItem>
                <MenuItem value="median">Median</MenuItem>
                <MenuItem value="mode">Mode</MenuItem>
                <MenuItem value="constant">Constant</MenuItem>
              </Select>
            </FormControl>
          )}
          {action === 'impute' && imputeMethod === 'constant' && (
            <TextField
              label="Constant Value"
              size="small"
              sx={{ minWidth: 120, ml: 2 }}
              value={imputeConstant}
              onChange={e => setImputeConstant(e.target.value)}
            />
          )}
          <FormControl>
            <Button
              variant="contained"
              color="secondary"
              disabled={selectedColumns.length === 0 || !action}
              sx={{ ml: 2 }}
              onClick={handleRunPreprocessing}
            >
              Run Preprocessing
            </Button>
          </FormControl>
          <FormControl>
            <Button
              variant="outlined"
              color="secondary"
              sx={{ ml: 2 }}
              onClick={handleUndo}
              disabled={loading || !sessionId || !canUndo}
            >
              Undo
            </Button>
          </FormControl>
          <FormControl>
            <Button
              variant="outlined"
              color="primary"
              sx={{ ml: 2 }}
              onClick={handleExportCSV}
              disabled={columns.length === 0 || rows.length === 0}
            >
              Export as CSV
            </Button>
          </FormControl>
        </Box>
      )}
      {loading && <CircularProgress size={24} sx={{ ml: 2 }} />}
      {error && <Typography color="error" sx={{ mt: 2 }}>{error}</Typography>}
      {columns.length > 0 && (
        <Box sx={{ height: 400, width: '100%', mt: 3 }}>
          <DataGrid
            rows={rows}
            columns={columns}
            paginationModel={paginationModel}
            onPaginationModelChange={setPaginationModel}
            pageSizeOptions={[10]}
          />
        </Box>
      )}
      {/* Exploratory Statistics Dashboard */}
      {Object.keys(colStats).length > 0 && (
        <Box sx={{ mt: 4 }}>
          {renderRecommendations()}
          {/* Force heatmap to re-render by keying on columns */}
          <div key={Object.keys(colStats).join(',')}>
            {renderDataQualityHeatmap()}
          </div>
          <Typography variant="h6" gutterBottom>Exploratory Statistics Dashboard</Typography>
          {/* Removed renderMissingnessGlance() as requested */}
          <FormControl sx={{ minWidth: 300, mb: 2 }} size="small">
            <InputLabel id="stats-cols-label">Columns to Visualize</InputLabel>
            <Select
              labelId="stats-cols-label"
              multiple
              value={statsColumns}
              onChange={e => setStatsColumns(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
              input={<OutlinedInput label="Columns to Visualize" />}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {Object.keys(colStats).map(col => (
                <MenuItem key={col} value={col}>
                  <Checkbox checked={statsColumns.indexOf(col) > -1} />
                  <ListItemText primary={col} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
            {statsColumns.map(col => (
              <ResizableBox
                key={col}
                width={380}
                height={320}
                minConstraints={[260, 180]}
                maxConstraints={[900, 700]}
                resizeHandles={['se']}
              >
                <Box sx={{ minWidth: 220, maxWidth: 800, height: '100%', border: '1px solid #eee', borderRadius: 2, p: 2, mb: 2, position: 'relative', boxSizing: 'border-box', display: 'flex', flexDirection: 'column' }}>
                  <Typography variant="subtitle1" gutterBottom>
                    {col}
                    {hasDataIssue(col) && <WarningAmberIcon color="warning" fontSize="small" sx={{ ml: 1, verticalAlign: 'middle' }} titleAccess="High missingness" />}
                  </Typography>
                  <FormControl sx={{ minWidth: 160, mb: 1 }} size="small">
                    <InputLabel id={`viz-type-label-${col}`}>Visualization</InputLabel>
                    <Select
                      labelId={`viz-type-label-${col}`}
                      value={colVizTypes[col] || (getColType(col) === 'numeric' ? 'bar' : 'bar')}
                      label="Visualization"
                      onChange={e => setColVizTypes(v => ({ ...v, [col]: e.target.value }))}
                    >
                      {(['numeric', 'categorical'] as const).includes(getColType(col) as any)
                        ? vizOptions[getColType(col) as 'numeric' | 'categorical'].map((opt: { value: string; label: string }) => (
                            <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                          ))
                        : null}
                    </Select>
                  </FormControl>
                  <Box sx={{ mb: 1 }}>
                    <strong>Count:</strong> {colStats[col].count} &nbsp;|
                    <strong> Missing %:</strong> {colStats[col].missing_pct.toFixed(2)}% &nbsp;|
                    <strong> Unique:</strong> {colStats[col].unique}
                  </Box>
                  {colStats[col].mean !== undefined && (
                    <Box sx={{ mb: 1 }}>
                      <strong>Mean:</strong> {colStats[col].mean?.toFixed(2)} &nbsp;|
                      <strong> Median:</strong> {colStats[col].median?.toFixed(2)} &nbsp;|
                      <strong> Std:</strong> {colStats[col].std?.toFixed(2)} &nbsp;|
                      <strong> Min:</strong> {colStats[col].min?.toFixed(2)} &nbsp;|
                      <strong> Max:</strong> {colStats[col].max?.toFixed(2)}
                    </Box>
                  )}
                  {colStats[col].top !== undefined && (
                    <Box sx={{ mb: 1 }}>
                      <strong>Top:</strong> {colStats[col].top} &nbsp;|
                      <strong> Freq:</strong> {colStats[col].freq}
                    </Box>
                  )}
                  <Box sx={{ flex: 1, minHeight: 120, minWidth: 180 }}>
                    {renderColumnChart(col)}
                  </Box>
                </Box>
              </ResizableBox>
            ))}
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default DataUploadPreview;

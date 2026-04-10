// ════════════════════════════════════════════════════════════════════
// app.js  —  Scorecard Interactive Binning Webapp
// Dataiku Standard Webapp — JS tab
//
// KEY FIX for Dataiku's JS sandbox:
//   All functions called from HTML onclick= MUST be attached to window.
//   See bottom of file: window.functionName = functionName
// ════════════════════════════════════════════════════════════════════

// ── API helper ───────────────────────────────────────────────────────
async function api(path, body) {
  var url = dataiku.getWebAppBackendUrl(path);
  var options = { method: body ? 'POST' : 'GET', headers: { 'Accept': 'application/json' } };
  if (body) { options.headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(body); }
  var response = await fetch(url, options);
  var ct = response.headers.get('content-type') || '';
  if (!ct.includes('application/json')) {
    var t = await response.text();
    throw new Error('HTTP ' + response.status + ': ' + t.replace(/<[^>]+>/g,'').trim().slice(0,120));
  }
  var data = await response.json();
  if (data.error) throw new Error(data.error);
  return data;
}

// ── State ────────────────────────────────────────────────────────────
var currentVar      = '';
var currentType     = '';
var currentBins     = [];
var currentCuts     = [];
var currentCatGroups= null;
var fittedResults   = {};
var selectedRows    = new Set();
var catInfoCache    = {};
var catAssignments  = {};
var numCatGroups    = 0;
var selectedChips   = new Set();
var modalCutIdx     = -1;
var countChartInst  = null;
var woeChartInst    = null;

var COLORS  = ['#2980b9','#27ae60','#e67e22','#8e44ad','#e74c3c','#16a085','#d35400','#f39c12'];
var CHIP_BG = ['#d6eaf8','#d5f5e3','#fdebd0','#e8daef','#fadbd8','#d1f2eb'];

// ── Loading overlay ──────────────────────────────────────────────────
function setLoading(on, msg) {
  var spin = document.getElementById('runSpin');
  if (spin) spin.style.display = on ? 'inline-block' : 'none';
  var ov = document.getElementById('loadingOverlay');
  if (on) {
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'loadingOverlay';
      ov.style.cssText = 'position:fixed;inset:0;background:rgba(255,255,255,.88);z-index:9999;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:16px';
      ov.innerHTML = '<div style="width:48px;height:48px;border:4px solid #e5e7eb;border-top-color:#1a6fc4;border-radius:50%;animation:sp .8s linear infinite"></div><div id="loadingMsg" style="font-size:14px;font-weight:600;color:#1a6fc4">Processing\u2026</div>';
      document.body.appendChild(ov);
    } else { ov.style.display = 'flex'; }
    var m = document.getElementById('loadingMsg');
    if (m && msg) m.textContent = msg;
  } else {
    if (ov) ov.style.display = 'none';
  }
}

function toast(msg, type) {
  var el = document.getElementById('toast');
  if (!el) return;
  el.textContent = msg;
  el.style.background = type === 'error' ? '#c0392b' : '#1a1a2e';
  el.style.opacity = '1';
  clearTimeout(el._t);
  el._t = setTimeout(function() { el.style.opacity = '0'; }, 3500);
}

function gv(id) { return document.getElementById(id).value; }

// ════════════════════════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════════════════════════
window.addEventListener('DOMContentLoaded', async function() {
  var dsSel = document.getElementById('datasetSel');
  try {
    var r = await api('/api/datasets');
    dsSel.innerHTML = '<option value="">— select dataset —</option>';
    r.datasets.forEach(function(d) {
      var o = document.createElement('option');
      o.value = o.textContent = d;
      dsSel.appendChild(o);
    });
  } catch(e) {
    dsSel.innerHTML = '<option value="">\u26a0 ' + e.message.slice(0,60) + '</option>';
  }

  dsSel.addEventListener('change', async function() {
    if (!this.value) return;
    try {
      var r = await api('/api/columns?dataset=' + encodeURIComponent(this.value));
      var t = document.getElementById('targetSel');
      t.innerHTML = r.columns.map(function(c) {
        return '<option value="' + c + '">' + c + '</option>';
      }).join('');
    } catch(e) { console.error('columns:', e.message); }
  });

  updateRecipeSnippet();
});

// ════════════════════════════════════════════════════════════════════
// DATASET LOADING
// ════════════════════════════════════════════════════════════════════
async function loadDataset() {
  var dataset = document.getElementById('datasetSel').value;
  var target  = document.getElementById('targetSel').value;
  if (!dataset) { toast('Select a dataset.', 'error'); return; }
  if (!target)  { toast('Select a target column.', 'error'); return; }

  setLoading(true, 'Loading dataset\u2026');
  try {
    var r = await api('/api/load', { dataset: dataset, target: target });
    fittedResults = {}; currentVar = ''; currentBins = []; selectedRows.clear();

    document.getElementById('dsStats').innerHTML =
      '<span class="ds-stat ds-green">' + r.n_rows.toLocaleString() + ' rows</span>' +
      '<span class="ds-stat ds-blue">Event rate ' + (r.event_rate * 100).toFixed(1) + '%</span>';

    var varSel = document.getElementById('varSel');
    varSel.innerHTML = '<option value="">— select variable —</option>';
    r.numeric_cols.concat(r.cat_cols).forEach(function(c) {
      var o = document.createElement('option'); o.value = o.textContent = c; varSel.appendChild(o);
    });

    var tSel = document.getElementById('targetSel');
    tSel.innerHTML = '';
    r.all_cols.forEach(function(c) {
      var o = document.createElement('option'); o.value = o.textContent = c;
      if (c === target) o.selected = true; tSel.appendChild(o);
    });

    buildVarList(r.numeric_cols, r.cat_cols);
    clearUI();
    toast('Loaded "' + dataset + '" — ' + r.numeric_cols.length + ' numeric, ' + r.cat_cols.length + ' categorical.');
  } catch(e) {
    toast('Load failed: ' + e.message, 'error');
  } finally {
    setLoading(false);
  }
}

// ════════════════════════════════════════════════════════════════════
// VARIABLE LIST
// ════════════════════════════════════════════════════════════════════
function buildVarList(numCols, catCols) {
  var cats = new Set(catCols);
  var all  = numCols.concat(catCols);
  var ivVals = Object.values(fittedResults).map(function(r) { return r.iv || 0; });
  var maxIV  = Math.max.apply(null, ivVals.concat([0.01]));
  var list   = document.getElementById('varList');
  list.innerHTML = '';

  all.forEach(function(vn) {
    var fit   = fittedResults[vn];
    var isNum = !cats.has(vn);
    var el    = document.createElement('div');
    el.className = 'vi' + (currentVar === vn ? ' active' : '');
    el.id = 'vi_' + vn;
    var ivBar = fit ? '<div class="iv-bar-bg"><div class="iv-bar-fg" style="width:' + Math.min(100, fit.iv / maxIV * 100).toFixed(0) + '%"></div></div>' : '';
    var ivNum = fit ? '<span class="vi-iv">' + fit.iv.toFixed(3) + '</span>' : '';
    el.innerHTML = '<span class="type-pill ' + (isNum ? 'tn' : 'tc') + '">' + (isNum ? 'N' : 'C') + '</span>' +
      '<div style="flex:1;min-width:0"><div class="vi-name">' + vn + '</div>' + ivBar + '</div>' + ivNum;
    el.onclick = function() {
      document.getElementById('varSel').value = vn;
      onVarChange();
      fitCurrent();
    };
    list.appendChild(el);
  });
}

function rebuildVarList() {
  var allVis = document.querySelectorAll('.vi');
  var ivVals = Object.values(fittedResults).map(function(r) { return r.iv || 0; });
  var maxIV  = Math.max.apply(null, ivVals.concat([0.01]));
  allVis.forEach(function(el) {
    var vn  = el.id.replace('vi_', '');
    var fit = fittedResults[vn];
    el.className = 'vi' + (currentVar === vn ? ' active' : '');
    if (fit) {
      var bar = el.querySelector('.iv-bar-fg');
      var spn = el.querySelector('.vi-iv');
      if (bar) bar.style.width = Math.min(100, fit.iv / maxIV * 100).toFixed(0) + '%';
      if (spn) spn.textContent = fit.iv.toFixed(3);
    }
  });
}

// ════════════════════════════════════════════════════════════════════
// VARIABLE CHANGE
// ════════════════════════════════════════════════════════════════════
async function onVarChange() {
  var vn = gv('varSel');
  if (!vn) return;
  currentVar = vn;
  selectedRows.clear();
  try {
    var r = await api('/api/variable_info?variable=' + encodeURIComponent(vn));
    currentType = r.dtype;
    if (r.dtype === 'categorical') catInfoCache[vn] = r.categories;
  } catch(e) { currentType = 'numerical'; }

  var isNum = currentType === 'numerical';
  document.getElementById('monoWrap').style.display   = isNum ? 'flex' : 'none';
  document.getElementById('catCutWrap').style.display = isNum ? 'none' : 'flex';
  document.getElementById('dtypePillWrap').innerHTML  =
    '<span class="dtype-pill ' + (isNum ? 'pn' : 'pc') + '">' + (isNum ? 'Numerical' : 'Categorical') + '</span>';

  document.querySelectorAll('.vi').forEach(function(el) { el.classList.remove('active'); });
  var eli = document.getElementById('vi_' + vn);
  if (eli) eli.classList.add('active');
}

// ════════════════════════════════════════════════════════════════════
// FIT CURRENT VARIABLE
// ════════════════════════════════════════════════════════════════════
async function fitCurrent() {
  var vn = gv('varSel');
  if (!vn) { toast('Select a variable first.'); return; }

  setLoading(true, 'Running optbinning\u2026');
  try {
    var result = await api('/api/fit', {
      variable:   vn,
      dtype:      currentType || null,
      max_bins:   parseInt(gv('binSlider')),
      monotonic:  gv('monoSel'),
      metric:     gv('metricSel'),
      cat_cutoff: parseInt(gv('catSlider') || '5') / 100,
    });
    currentVar  = vn;
    currentType = result.dtype;
    currentBins = result.bins;
    currentCuts = result.cutoffs || [];
    currentCatGroups = result.cat_groups || null;
    selectedRows.clear();
    fittedResults[vn] = result;

    if (result.dtype === 'categorical' && !catInfoCache[vn]) {
      try {
        var info = await api('/api/variable_info?variable=' + encodeURIComponent(vn));
        catInfoCache[vn] = info.categories;
      } catch(e) {}
    }

    renderAll(result);
    rebuildVarList();
    refreshExportSelectors();
  } catch(e) {
    toast('Fit failed: ' + e.message, 'error');
  } finally {
    setLoading(false);
  }
}

// ════════════════════════════════════════════════════════════════════
// FIT ALL
// ════════════════════════════════════════════════════════════════════
async function fitAll() {
  var catVars = Object.entries(fittedResults)
    .filter(function(kv) { return kv[1].dtype === 'categorical'; })
    .map(function(kv) { return kv[0]; });

  setLoading(true, 'Fitting all variables\u2026');
  try {
    var r = await api('/api/fit_all', {
      max_bins:  parseInt(gv('binSlider')),
      monotonic: gv('monoSel'),
      metric:    gv('metricSel'),
      cat_vars:  catVars,
    });
    r.summary.forEach(function(s) {
      if (!fittedResults[s.Variable]) fittedResults[s.Variable] = {};
      fittedResults[s.Variable].iv    = s.IV;
      fittedResults[s.Variable].dtype = s.Type;
    });
    if (currentVar && fittedResults[currentVar]) await fitCurrent();
    rebuildVarList();
    refreshExportSelectors();
    toast('Fitted ' + r.summary.length + ' variables.');
  } catch(e) {
    toast('Fit all failed: ' + e.message, 'error');
  } finally {
    setLoading(false);
  }
}

// ════════════════════════════════════════════════════════════════════
// RENDER
// ════════════════════════════════════════════════════════════════════
function renderAll(result) {
  renderMetrics(result);
  renderTable(result.bins);
  renderCharts(result.bins);
  var isCat = result.dtype === 'categorical';
  document.getElementById('tabCat').style.display  = isCat ? '' : 'none';
  document.getElementById('dragHint').style.display = isCat ? 'none' : '';
  if (isCat) { switchTab('cat'); buildCatUI(result.bins); }
  else        { switchTab('stats'); renderCutLines(result.bins); }
}

function renderMetrics(r) {
  document.getElementById('mIV').textContent   = r.iv.toFixed(3);
  document.getElementById('mGini').textContent = (r.gini * 100).toFixed(1) + '%';
  document.getElementById('mKS').textContent   = (r.ks * 100).toFixed(1) + '%';
  document.getElementById('mBins').textContent = r.bins.length;
  document.getElementById('mMono').textContent = r.is_monotonic
    ? (r.monotonic_direction === 'increasing' ? '\u2191 Yes' : '\u2193 Yes')
    : (r.dtype === 'categorical' ? 'N/A' : '\u2717 No');
  var interp = r.iv_interpretation || '';
  var badge  = document.getElementById('ivInterp');
  badge.style.display = 'inline-block';
  badge.textContent   = interp;
  var cls = {Useless:'ivu',Weak:'ivw',Medium:'ivm',Strong:'ivs','Very strong \u2014 check for leakage':'ivvs'};
  badge.className = 'iv-interp ' + (cls[interp] || 'ivw');
}

function renderTable(bins) {
  var tbody = document.getElementById('binTbody');
  tbody.innerHTML = '';
  bins.forEach(function(b, i) {
    var trend = '';
    if (i > 0) {
      var d = b.woe - bins[i-1].woe;
      trend = d > 0.01 ? '<span class="woe-pos">\u2191</span>' : d < -0.01 ? '<span class="woe-neg">\u2193</span>' : '<span class="woe-zero">\u2192</span>';
    }
    var wc  = b.woe > 0.01 ? 'woe-pos' : b.woe < -0.01 ? 'woe-neg' : 'woe-zero';
    var lbl = b.categories
      ? b.categories.map(function(c) { return '<span class="cat-chip">' + c + '</span>'; }).join('')
      : b.label;
    var isSel = selectedRows.has(b.group);
    var rowActions = '';
    if (currentType === 'numerical') {
      if (i < bins.length - 1) rowActions += '<button onclick="openThreshModal(' + i + ')" style="font-size:10px;padding:2px 6px" title="Edit boundary">\u270e</button> ';
      if (bins.length >= 2)    rowActions += '<button onclick="splitBinAt(' + i + ')" style="font-size:10px;padding:2px 6px" title="Split bin">\u2295</button>';
    }
    var tr = document.createElement('tr');
    tr.className    = 'bin-row' + (isSel ? ' selected' : '');
    tr.dataset.group = b.group;
    tr.innerHTML =
      '<td><input type="checkbox"' + (isSel ? ' checked' : '') + ' onclick="toggleRow(event,' + b.group + ')"></td>' +
      '<td style="max-width:220px;white-space:normal">' + lbl + '</td>' +
      '<td>' + b.group + '</td>' +
      '<td>' + b.count.toLocaleString() + '</td>' +
      '<td>' + b.event_count.toLocaleString() + '</td>' +
      '<td>' + b.non_event_count.toLocaleString() + '</td>' +
      '<td>' + (b.event_rate * 100).toFixed(1) + '%</td>' +
      '<td class="' + wc + '">' + b.woe.toFixed(4) + '</td>' +
      '<td>' + b.iv_contribution.toFixed(4) + '</td>' +
      '<td>' + trend + '</td>' +
      '<td style="white-space:nowrap">' + rowActions + '</td>';
    tr.addEventListener('click', function(e) {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON') return;
      toggleRowClick(e, b.group);
    });
    tbody.appendChild(tr);
  });
  updateBinToolbar();
}

function toggleRow(e, group) { e.stopPropagation(); toggleRowClick(e, group); }

function toggleRowClick(e, group) {
  if (e.shiftKey || e.ctrlKey || e.metaKey || e.target.tagName === 'INPUT') {
    if (selectedRows.has(group)) selectedRows.delete(group); else selectedRows.add(group);
  } else {
    if (selectedRows.size === 1 && selectedRows.has(group)) selectedRows.clear();
    else { selectedRows.clear(); selectedRows.add(group); }
  }
  document.querySelectorAll('.bin-row').forEach(function(tr) {
    var g  = parseInt(tr.dataset.group);
    var sel = selectedRows.has(g);
    tr.classList.toggle('selected', sel);
    var cb = tr.querySelector('input[type=checkbox]');
    if (cb) cb.checked = sel;
  });
  updateBinToolbar();
  highlightChartBars();
}

function updateBinToolbar() {
  var toolbar = document.getElementById('binToolbar');
  var hint    = document.getElementById('binHint');
  toolbar.querySelectorAll('.tb-action').forEach(function(el) { el.remove(); });
  if (selectedRows.size === 0) {
    hint.style.display = '';
    hint.textContent = 'Click a row to select \u00b7 Shift/Ctrl+click for multi-select';
    return;
  }
  hint.style.display = 'none';
  var sorted = Array.from(selectedRows).sort(function(a,b){return a-b;});
  var n = sorted.length;

  var lbl = document.createElement('span');
  lbl.className = 'tb-action';
  lbl.style.cssText = 'font-size:11px;color:#1a6fc4;font-weight:600';
  lbl.textContent = n + ' bin' + (n>1?'s':'') + ' selected (Group' + (n>1?'s':'') + ' ' + sorted.join(', ') + ')';
  toolbar.appendChild(lbl);

  if (n >= 2) {
    var contiguous = sorted.every(function(g,i) { return i===0 || g===sorted[i-1]+1; });
    var btn = document.createElement('button');
    btn.className = 'tb-action primary';
    btn.style.fontSize = '11px';
    btn.textContent = '\u2295 Merge ' + n + ' bins';
    btn.disabled    = !contiguous;
    btn.title       = contiguous ? 'Merge into one bin' : 'Only adjacent bins can be merged';
    btn.onclick     = mergeBins;
    toolbar.appendChild(btn);
    if (!contiguous) {
      var w = document.createElement('span');
      w.className = 'tb-action';
      w.style.cssText = 'font-size:10px;color:#e74c3c;font-style:italic';
      w.textContent = '(select adjacent bins only)';
      toolbar.appendChild(w);
    }
  }

  if (n === 1 && currentType === 'numerical') {
    var idx = currentBins.findIndex(function(b) { return b.group === sorted[0]; });
    if (idx >= 0 && idx < currentBins.length - 1) {
      var bt = document.createElement('button');
      bt.className = 'tb-action ghost'; bt.style.fontSize = '11px';
      bt.textContent = '\u270e Edit boundary';
      bt.onclick = function() { openThreshModal(idx); };
      toolbar.appendChild(bt);
    }
    var bs = document.createElement('button');
    bs.className = 'tb-action ghost'; bs.style.fontSize = '11px';
    bs.textContent = '\u2295 Split bin';
    bs.onclick = function() { splitBinAt(currentBins.findIndex(function(b){ return b.group===sorted[0]; })); };
    toolbar.appendChild(bs);
  }

  var clr = document.createElement('button');
  clr.className = 'tb-action'; clr.style.fontSize = '11px';
  clr.textContent = 'Clear selection';
  clr.onclick = function() { selectedRows.clear(); renderTable(currentBins); };
  toolbar.appendChild(clr);
}

// ════════════════════════════════════════════════════════════════════
// CHARTS
// ════════════════════════════════════════════════════════════════════
function renderCharts(bins) {
  if (countChartInst) { countChartInst.destroy(); countChartInst = null; }
  if (woeChartInst)   { woeChartInst.destroy();   woeChartInst   = null; }
  var labels = bins.map(function(b) { return 'G' + b.group; });
  var bg     = bins.map(function(_,i) { return COLORS[i % COLORS.length]; });

  countChartInst = new Chart(document.getElementById('countChart'), {
    type: 'bar',
    data: { labels: labels, datasets: [
      { label:'Events',     data: bins.map(function(b){return b.event_count;}),     backgroundColor: bg,                           stack:'s' },
      { label:'Non-events', data: bins.map(function(b){return b.non_event_count;}), backgroundColor: bg.map(function(c){return c+'55';}), stack:'s' },
    ]},
    options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      onClick: function(_,els) {
        if (els.length) toggleRowClick({shiftKey:false,ctrlKey:false,metaKey:false,target:{}}, bins[els[0].index].group);
      },
      scales: { x:{ticks:{font:{size:10},maxRotation:25}}, y:{ticks:{font:{size:10}}} }
    }
  });

  var wd = bins.map(function(b) { return +b.woe.toFixed(4); });
  woeChartInst = new Chart(document.getElementById('woeChart'), {
    type: 'bar',
    data: { labels: labels, datasets: [{ label:'WOE', data:wd,
      backgroundColor: wd.map(function(w){return w>=0?'#27ae6088':'#e74c3c88';}),
      borderColor:     wd.map(function(w){return w>=0?'#27ae60':'#e74c3c';}),
      borderWidth: 1.5 }] },
    options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales: { x:{ticks:{font:{size:10}}}, y:{ticks:{font:{size:10}}} } }
  });
}

function highlightChartBars() {
  if (!countChartInst) return;
  var bg = currentBins.map(function(_,i) { return COLORS[i % COLORS.length]; });
  countChartInst.data.datasets[0].backgroundColor = bg.map(function(c,i) {
    return selectedRows.size === 0 || selectedRows.has(currentBins[i].group) ? c : c + '44';
  });
  countChartInst.data.datasets[1].backgroundColor = bg.map(function(c,i) {
    return selectedRows.size === 0 || selectedRows.has(currentBins[i].group) ? c+'55' : c+'22';
  });
  countChartInst.update('none');
}

// ════════════════════════════════════════════════════════════════════
// DRAGGABLE CUTOFF LINES
// ════════════════════════════════════════════════════════════════════
function renderCutLines(bins) {
  var container = document.getElementById('cutLines');
  container.innerHTML = '';
  if (!bins.length || currentType === 'categorical' || !currentCuts.length) return;
  var xMin  = bins[0].lower;
  var xMax  = bins[bins.length-1].upper;
  var range = xMax - xMin;

  currentCuts.forEach(function(cut, idx) {
    var h    = (document.querySelector('.chart-wrap') || {offsetHeight:185}).offsetHeight || 185;
    var line = document.createElement('div');
    line.className  = 'cutoff-line';
    line.style.cssText = 'left:' + ((cut-xMin)/range*100).toFixed(2) + '%;top:0;height:' + (h-28) + 'px';

    line.addEventListener('mousedown', function(e) {
      e.preventDefault();
      var startX   = e.clientX;
      var startCut = currentCuts[idx];
      var outerW   = (document.getElementById('chartOuter') || {offsetWidth:600}).offsetWidth;
      var tip      = document.getElementById('cutTip');

      function onMove(ev) {
        var delta = ((ev.clientX - startX) / outerW) * range;
        var lo    = idx > 0 ? currentCuts[idx-1] + range*0.005 : xMin + range*0.005;
        var hi    = idx < currentCuts.length-1 ? currentCuts[idx+1] - range*0.005 : xMax - range*0.005;
        var nc    = Math.max(lo, Math.min(hi, startCut + delta));
        currentCuts[idx]   = nc;
        line.style.left    = ((nc-xMin)/range*100).toFixed(2) + '%';
        tip.style.display  = 'block';
        tip.style.left     = line.style.left;
        tip.style.top      = '4px';
        tip.textContent    = nc.toFixed(2);
      }

      async function onUp() {
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup',  onUp);
        tip.style.display = 'none';
        selectedRows.clear();
        setLoading(true, 'Recomputing\u2026');
        try {
          var result = await api('/api/adjust', { variable: currentVar, cutoffs: currentCuts });
          applyResult(result, null);
        } finally { setLoading(false); }
      }

      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup',   onUp);
    });

    container.appendChild(line);
  });
}

// ════════════════════════════════════════════════════════════════════
// BIN OPERATIONS
// ════════════════════════════════════════════════════════════════════
async function mergeBins() {
  var sorted = Array.from(selectedRows).sort(function(a,b){return a-b;});
  if (currentType === 'numerical') {
    var firstIdx = sorted[0] - 1;
    var lastIdx  = sorted[sorted.length-1] - 1;
    var newCuts  = currentCuts.filter(function(_,i) { return i < firstIdx || i >= lastIdx; });
    selectedRows.clear();
    setLoading(true, 'Merging bins\u2026');
    try { var r = await api('/api/adjust', {variable:currentVar, cutoffs:newCuts}); applyResult(r, 'Merged ' + sorted.length + ' bins.'); }
    finally { setLoading(false); }
  } else {
    var firstGid = sorted[0];
    var newAsgn  = {};
    currentBins.forEach(function(b) {
      if (!b.categories) return;
      var tgt = sorted.indexOf(b.group) >= 0 ? firstGid : b.group;
      b.categories.forEach(function(c) { newAsgn[c] = tgt; });
    });
    selectedRows.clear();
    setLoading(true, 'Merging groups\u2026');
    try { var r = await api('/api/merge_categories', {variable:currentVar, group_assignments:newAsgn}); applyResult(r, 'Merged ' + sorted.length + ' groups.'); }
    finally { setLoading(false); }
  }
}

async function splitBinAt(binIdx) {
  var b = currentBins[binIdx];
  if (!b) return;
  var mid     = (b.lower + b.upper) / 2;
  var newCuts = currentCuts.concat([mid]).sort(function(a,b){return a-b;});
  selectedRows.clear();
  setLoading(true, 'Splitting bin\u2026');
  try { var r = await api('/api/adjust', {variable:currentVar, cutoffs:newCuts}); applyResult(r, 'Split bin ' + b.group + ' at ' + mid.toFixed(2) + '.'); }
  finally { setLoading(false); }
}

function openThreshModal(cutIdx) {
  modalCutIdx = cutIdx;
  var lo  = cutIdx > 0 ? currentCuts[cutIdx-1] : currentBins[0].lower;
  var hi  = cutIdx < currentCuts.length-1 ? currentCuts[cutIdx+1] : currentBins[currentBins.length-1].upper;
  document.getElementById('modalGrpA').textContent      = cutIdx + 1;
  document.getElementById('modalGrpB').textContent      = cutIdx + 2;
  document.getElementById('modalCurrentVal').textContent = currentCuts[cutIdx].toFixed(4);
  document.getElementById('modalRange').textContent     = lo.toFixed(2) + ' \u2013 ' + hi.toFixed(2);
  document.getElementById('modalThreshInput').value     = currentCuts[cutIdx].toFixed(4);
  document.getElementById('thresholdModal').style.display = 'flex';
}

function closeModal() { document.getElementById('thresholdModal').style.display = 'none'; }

async function applyThreshold() {
  var val = parseFloat(document.getElementById('modalThreshInput').value);
  if (isNaN(val)) { toast('Enter a valid number.', 'error'); return; }
  var lo  = modalCutIdx > 0 ? currentCuts[modalCutIdx-1] : currentBins[0].lower;
  var hi  = modalCutIdx < currentCuts.length-1 ? currentCuts[modalCutIdx+1] : currentBins[currentBins.length-1].upper;
  if (val <= lo || val >= hi) { toast('Must be between ' + lo.toFixed(2) + ' and ' + hi.toFixed(2) + '.', 'error'); return; }
  var newCuts = currentCuts.slice();
  newCuts[modalCutIdx] = val;
  closeModal();
  selectedRows.clear();
  setLoading(true, 'Applying boundary\u2026');
  try { var r = await api('/api/adjust', {variable:currentVar, cutoffs:newCuts}); applyResult(r, 'Boundary set to ' + val.toFixed(2) + '.'); }
  finally { setLoading(false); }
}

function applyResult(result, msg) {
  currentBins      = result.bins;
  currentCuts      = result.cutoffs || [];
  currentCatGroups = result.cat_groups || null;
  fittedResults[currentVar] = result;
  renderAll(result);
  rebuildVarList();
  refreshExportSelectors();
  if (msg) toast(msg);
}

// ════════════════════════════════════════════════════════════════════
// CATEGORICAL GROUPING UI
// ════════════════════════════════════════════════════════════════════
function buildCatUI(bins) {
  catAssignments = {}; numCatGroups = 0;
  bins.forEach(function(b) {
    if (b.categories) {
      b.categories.forEach(function(c) { catAssignments[c] = b.group; });
      numCatGroups = Math.max(numCatGroups, b.group);
    }
  });
  selectedChips.clear();
  renderCatUI();
}

function renderCatUI() {
  var container = document.getElementById('catGroupsUI');
  var uaZone    = document.getElementById('uaZone');
  container.innerHTML = ''; uaZone.innerHTML = '';
  var info = catInfoCache[currentVar] || [];
  var groups = {}; for (var g=1;g<=numCatGroups;g++) groups[g]=[];
  var unassigned = [];
  info.forEach(function(co) {
    var gid = catAssignments[co.value];
    if (gid && groups[gid]) groups[gid].push(co); else unassigned.push(co);
  });
  for (var g=1;g<=numCatGroups;g++) {
    var row   = document.createElement('div'); row.className = 'cat-group-row';
    var color = CHIP_BG[(g-1) % CHIP_BG.length];
    var border= COLORS[(g-1) % COLORS.length];
    row.innerHTML = '<div class="cat-group-lbl" style="color:' + border + '">Group ' + g + '</div>' +
      '<div class="chips-zone" id="grpz_' + g + '"></div>' +
      '<button onclick="moveChips(' + g + ')" style="font-size:10px;padding:2px 7px;flex-shrink:0;margin-top:3px">\u2190 Move here</button>';
    container.appendChild(row);
    (groups[g] || []).forEach(function(co) { document.getElementById('grpz_'+g).appendChild(mkChip(co, color)); });
  }
  unassigned.forEach(function(co) { uaZone.appendChild(mkChip(co, '#e8e8e8')); });
}

function mkChip(co, bg) {
  var chip = document.createElement('span');
  chip.className    = 'chip' + (selectedChips.has(co.value) ? ' selected' : '');
  chip.dataset.value = co.value;
  chip.style.cssText = 'background:' + bg + ';border-color:' + bg;
  chip.innerHTML    = co.value + '<span class="chip-rate">' + (co.event_rate*100).toFixed(1) + '%</span>';
  chip.onclick = function(e) {
    if (e.ctrlKey || e.metaKey || e.shiftKey) {
      if (selectedChips.has(co.value)) selectedChips.delete(co.value); else selectedChips.add(co.value);
    } else {
      if (selectedChips.has(co.value) && selectedChips.size===1) selectedChips.clear();
      else { selectedChips.clear(); selectedChips.add(co.value); }
    }
    renderCatUI();
  };
  return chip;
}

function moveChips(gid) {
  if (!selectedChips.size) { toast('Click a chip to select it first.'); return; }
  selectedChips.forEach(function(c) { catAssignments[c] = gid; });
  selectedChips.clear(); renderCatUI();
}

function addCatGroup()  { numCatGroups++; renderCatUI(); }

async function applyGrouping() {
  var info = catInfoCache[currentVar] || [];
  info.forEach(function(co) { if (!catAssignments[co.value]) catAssignments[co.value] = 1; });
  selectedChips.clear();
  setLoading(true, 'Applying grouping\u2026');
  try { var r = await api('/api/merge_categories', {variable:currentVar, group_assignments:catAssignments}); applyResult(r, 'Grouping applied.'); }
  finally { setLoading(false); }
}

// ════════════════════════════════════════════════════════════════════
// WRITE OUTPUT DATASET
// ════════════════════════════════════════════════════════════════════
async function writeOutputDataset() {
  if (!Object.keys(fittedResults).length) { toast('Fit at least one variable first.'); return; }
  var name = prompt('Output dataset name:', 'woe_output');
  if (!name) return;
  setLoading(true, 'Writing output dataset\u2026');
  try {
    var r = await api('/api/transform', { output_dataset: name, metrics: ['woe','group','label'] });
    toast('Written \u2192 "' + r.output_dataset + '" \u2014 ' + r.new_columns.length + ' new columns.');
  } catch(e) { toast('Write failed: ' + e.message, 'error'); }
  finally { setLoading(false); }
}

// ════════════════════════════════════════════════════════════════════
// EXPORT
// ════════════════════════════════════════════════════════════════════
function refreshExportSelectors() {
  ['pyVarSel','sqlVarSel'].forEach(function(id) {
    var sel = document.getElementById(id);
    if (!sel) return;
    var cur = sel.value;
    sel.innerHTML = '';
    Object.keys(fittedResults).forEach(function(vn) {
      var o = document.createElement('option'); o.value = o.textContent = vn; sel.appendChild(o);
    });
    if (fittedResults[cur]) sel.value = cur;
    else if (currentVar && fittedResults[currentVar]) sel.value = currentVar;
  });
  loadPyPrev(); loadSQLPrev();
}

async function loadPyPrev() {
  var vn = (document.getElementById('pyVarSel') || {value:''}).value;
  if (!vn) return;
  try { var r = await api('/api/preview/python?variable=' + encodeURIComponent(vn));
    document.getElementById('pyPrev').innerHTML = hPy(r.code); } catch(e) {}
}

async function loadSQLPrev() {
  var vn = (document.getElementById('sqlVarSel') || {value:''}).value;
  var d  = (document.getElementById('sqlDialect') || {value:'standard'}).value;
  if (!vn) return;
  try { var r = await api('/api/preview/sql?variable=' + encodeURIComponent(vn) + '&dialect=' + d);
    document.getElementById('sqlPrev').innerHTML = hSQL(r.sql); } catch(e) {}
}

async function dlFile(path, filename) {
  if (!Object.keys(fittedResults).length) { toast('Fit variables first.'); return; }
  var url  = dataiku.getWebAppBackendUrl(path);
  var res  = await fetch(url);
  var text = await res.text();
  if (filename.endsWith('.json')) document.getElementById('jsonPrev').innerHTML = hJSON(text.slice(0,800));
  dl(filename, text, 'text/plain');
  toast('Downloaded ' + filename);
}

async function dlSQL() {
  var d = (document.getElementById('sqlDialect') || {value:'standard'}).value;
  await dlFile('/api/export/sql?dialect=' + d + '&source_table=input_table', 'transform.sql');
}

async function dlScorecard() {
  if (!Object.keys(fittedResults).length) { toast('Fit variables first.'); return; }
  var pdo = (document.getElementById('pdoIn') || {value:'20'}).value;
  var bs  = (document.getElementById('baseIn') || {value:'600'}).value;
  var url = dataiku.getWebAppBackendUrl('/api/export/scorecard?pdo=' + pdo + '&base_score=' + bs);
  var res = await fetch(url);
  var csv = await res.text();
  document.getElementById('scPrev').textContent = csv.split('\n').slice(0,10).join('\n') + '...';
  dl('scorecard_table.csv', csv, 'text/csv');
  toast('Downloaded scorecard_table.csv');
}

async function scoreRecord() {
  var raw = document.getElementById('testInput').value.trim();
  var rec; try { rec = JSON.parse(raw); } catch(e) { toast('Invalid JSON', 'error'); return; }
  if (!Object.keys(fittedResults).length) { toast('Fit at least one variable first.'); return; }
  try {
    var r  = await api('/api/score_record', { record: rec });
    var el = document.getElementById('scoreOut');
    el.style.display = 'block';
    el.textContent   = JSON.stringify(r.output, null, 2);
  } catch(e) { toast('Score failed: ' + e.message, 'error'); }
}

function updateRecipeSnippet() {
  var el = document.getElementById('recipeSnip');
  if (!el) return;
  el.innerHTML = hPy('import dataiku, json\nfrom binning_engine import ScoringBundle\n\ninput_ds  = dataiku.Dataset(get_input_names_for_role(\'input\')[0])\noutput_ds = dataiku.Dataset(get_output_names_for_role(\'output\')[0])\nparams      = get_recipe_config()\nbundle_dict = json.loads(params[\'bundle_json\'])\n\nbundle  = ScoringBundle.load_json_dict(bundle_dict)\ndf_in   = input_ds.get_dataframe()\ndf_out  = bundle.score_dataframe(df_in, metrics=[\'woe\', \'group\'])\noutput_ds.write_with_schema(df_out)');
}

// ════════════════════════════════════════════════════════════════════
// TABS / PANELS
// ════════════════════════════════════════════════════════════════════
function switchMain(tab) {
  document.getElementById('mtBin').className = 'main-tab' + (tab==='binning'?' active':'');
  document.getElementById('mtExp').className = 'main-tab' + (tab==='export'?' active':'');
  document.getElementById('panelBinning').style.display = tab==='binning'?'flex':'none';
  document.getElementById('panelExport').style.display  = tab==='export'?'flex':'none';
  if (tab==='export') { refreshExportSelectors(); updateRecipeSnippet(); }
}
function switchTab(tab) {
  document.getElementById('tabStats').className = 'tab-btn'+(tab==='stats'?' active':'');
  document.getElementById('tabCat').className   = 'tab-btn'+(tab==='cat'?' active':'');
  document.getElementById('panelStats').style.display = tab==='stats'?'':'none';
  document.getElementById('panelCat').style.display   = tab==='cat'?'':'none';
}

// ════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════
function clearUI() {
  document.getElementById('binTbody').innerHTML =
    '<tr><td colspan="11" style="text-align:center;color:#aaa;padding:24px;font-size:12px">Select a variable and click Run binning</td></tr>';
  ['mIV','mGini','mKS','mBins','mMono'].forEach(function(id) { document.getElementById(id).textContent = '\u2014'; });
  document.getElementById('ivInterp').style.display = 'none';
  if (countChartInst) { countChartInst.destroy(); countChartInst = null; }
  if (woeChartInst)   { woeChartInst.destroy();   woeChartInst   = null; }
  document.getElementById('cutLines').innerHTML = '';
  selectedRows.clear();
}

var esc = function(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
function hPy(c)  { return esc(c).replace(/("""[\s\S]*?""")/g,'<span class="str">$1</span>').replace(/(#[^\n]*)/g,'<span class="cm">$1</span>').replace(/\b(def|return|if|elif|else|import|from|in|not|True|False|None|try|except|for|class)\b/g,'<span class="kw">$1</span>').replace(/\b([a-z_]\w*)\s*(?=\()/g,'<span class="fn">$1</span>').replace(/\b(\d+\.?\d*)\b/g,'<span class="num">$1</span>'); }
function hSQL(c) { return esc(c).replace(/(--[^\n]*)/g,'<span class="cm">$1</span>').replace(/\b(SELECT|FROM|CASE|WHEN|THEN|ELSE|END|AS|IS|NULL|IN|AND|OR)\b/g,'<span class="kw">$1</span>').replace(/'([^']*)'/g,'<span class="str">\'$1\'</span>').replace(/\b(\d+\.?\d*)\b/g,'<span class="num">$1</span>'); }
function hJSON(c){ return esc(c).replace(/"([^"]+)":/g,'"<span class="fn">$1</span>":').replace(/:\s*"([^"]*)"/g,': "<span class="str">$1</span>"').replace(/:\s*(\d+\.?\d*)/g,': <span class="num">$1</span>'); }
function mk(tag,cls,txt) { var el=document.createElement(tag); if(cls)el.className=cls; if(txt)el.textContent=txt; return el; }
function dl(name,content,type) { var a=document.createElement('a'); a.href=URL.createObjectURL(new Blob([content],{type:type})); a.download=name; a.click(); }

// ════════════════════════════════════════════════════════════════════
// EXPOSE ALL FUNCTIONS TO GLOBAL SCOPE
// Dataiku's JS sandbox requires this for onclick= attributes to work
// ════════════════════════════════════════════════════════════════════
window.loadDataset       = loadDataset;
window.fitCurrent        = fitCurrent;
window.fitAll            = fitAll;
window.onVarChange       = onVarChange;
window.switchMain        = switchMain;
window.switchTab         = switchTab;
window.closeModal        = closeModal;
window.applyThreshold    = applyThreshold;
window.openThreshModal   = openThreshModal;
window.splitBinAt        = splitBinAt;
window.mergeBins         = mergeBins;
window.addCatGroup       = addCatGroup;
window.applyGrouping     = applyGrouping;
window.moveChips         = moveChips;
window.writeOutputDataset= writeOutputDataset;
window.loadPyPrev        = loadPyPrev;
window.loadSQLPrev       = loadSQLPrev;
window.dlFile            = dlFile;
window.dlSQL             = dlSQL;
window.dlScorecard       = dlScorecard;
window.scoreRecord       = scoreRecord;
window.toggleRow         = toggleRow;
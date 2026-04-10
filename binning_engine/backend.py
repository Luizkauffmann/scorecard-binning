# ════════════════════════════════════════════════════════════════════
# backend.py — Scorecard Interactive Binning (Dataiku Standard Webapp)
# Paste into the Python tab. Dataiku provides `app` — do NOT create it.
# ════════════════════════════════════════════════════════════════════

import json
import traceback
import pandas as pd
import numpy as np
import dataiku
from flask import request, jsonify, Response

# ── Import binning engine — with visible error if it fails ───────────
try:
    from binning_engine.binning_engine import BinningEngine, ScoringBundle, ScoringArtifact, interpret_iv
    _engine_import_error = None
except Exception as _e:
    _engine_import_error = str(_e)
    traceback.print_exc()

# ── Global state ─────────────────────────────────────────────────────
_engine = None
_df     = None
_target = None
_dsname = None


def _require_engine():
    if _engine_import_error:
        raise RuntimeError("binning_engine import failed: " + _engine_import_error)
    if _engine is None:
        raise RuntimeError("No dataset loaded — call /api/load first.")


# ════════════════════════════════════════════════════════════════════
# HEALTH / DEBUG
# ════════════════════════════════════════════════════════════════════

@app.route('/api/ping')
def api_ping():
    return jsonify(
        status  = 'ok',
        engine_import_error = _engine_import_error,
        dataset_loaded = _df is not None,
    )


# ════════════════════════════════════════════════════════════════════
# DATASET ROUTES
# ════════════════════════════════════════════════════════════════════

@app.route('/api/datasets')
def api_datasets():
    try:
        project  = dataiku.api_client().get_default_project()
        datasets = sorted(d['name'] for d in project.list_datasets())
        return jsonify(datasets=datasets)
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route('/api/columns')
def api_columns():
    dsname = request.args.get('dataset')
    if not dsname:
        return jsonify(error='dataset param required'), 400
    try:
        schema  = dataiku.Dataset(dsname).read_schema()
        columns = [col['name'] for col in schema]
        return jsonify(columns=columns)
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/load', methods=['POST'])
def api_load():
    global _engine, _df, _target, _dsname
    if _engine_import_error:
        return jsonify(error="binning_engine import failed: " + _engine_import_error), 500
    body   = request.get_json()
    dsname = body.get('dataset')
    target = body.get('target')
    try:
        _df     = dataiku.Dataset(dsname).get_dataframe()
        _dsname = dsname
        if target not in _df.columns:
            return jsonify(error=f"Column '{target}' not found."), 400
        _target = target
        _engine = BinningEngine(_df, target_col=target)
        num_cols, cat_cols = [], []
        for c in _df.columns:
            if c == target:
                continue
            if pd.api.types.is_numeric_dtype(_df[c]) and _df[c].nunique() > 10:
                num_cols.append(c)
            else:
                cat_cols.append(c)
        return jsonify(
            status       = 'ok',
            n_rows       = len(_df),
            event_rate   = round(float(_df[target].mean()), 4),
            numeric_cols = num_cols,
            cat_cols     = cat_cols,
            all_cols     = list(_df.columns),
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route('/api/variable_info')
def api_variable_info():
    _require_engine()
    var = request.args.get('variable')
    if not var or var not in _df.columns:
        return jsonify(error='variable not found'), 400
    try:
        col    = _df[var]
        is_num = pd.api.types.is_numeric_dtype(col) and col.nunique() > 10
        if is_num:
            return jsonify(dtype='numerical',
                           min=float(col.min()), max=float(col.max()),
                           mean=float(col.mean()), n_unique=int(col.nunique()))
        else:
            cats = []
            for val, grp in _df.groupby(var):
                ev = int(grp[_target].sum())
                n  = len(grp)
                cats.append(dict(value=str(val), count=n, events=ev,
                                 event_rate=round(ev/n, 4) if n else 0))
            cats.sort(key=lambda c: c['event_rate'])
            return jsonify(dtype='categorical', categories=cats)
    except Exception as e:
        return jsonify(error=str(e)), 500


# ════════════════════════════════════════════════════════════════════
# FITTING ROUTES
# ════════════════════════════════════════════════════════════════════

@app.route('/api/fit', methods=['POST'])
def api_fit():
    _require_engine()
    b = request.get_json()
    try:
        result = _engine.fit(
            variable   = b['variable'],
            dtype      = b.get('dtype'),
            max_bins   = int(b.get('max_bins', 6)),
            monotonic  = b.get('monotonic', 'none'),
            metric     = b.get('metric', 'iv'),
            cat_cutoff = float(b.get('cat_cutoff', 0.05)),
        )
        return jsonify(_serialize(result))
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route('/api/fit_all', methods=['POST'])
def api_fit_all():
    _require_engine()
    b = request.get_json()
    try:
        summary = _engine.fit_all(
            categorical_variables = b.get('cat_vars', []),
            max_bins  = int(b.get('max_bins', 6)),
            monotonic = b.get('monotonic', 'none'),
            metric    = b.get('metric', 'iv'),
        )
        return jsonify(summary=summary.to_dict(orient='records'))
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route('/api/adjust', methods=['POST'])
def api_adjust():
    _require_engine()
    b = request.get_json()
    try:
        result = _engine.adjust_cutoffs(
            variable    = b['variable'],
            new_cutoffs = [float(c) for c in b['cutoffs']],
        )
        return jsonify(_serialize(result))
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route('/api/merge_categories', methods=['POST'])
def api_merge_categories():
    _require_engine()
    b = request.get_json()
    try:
        result = _engine.merge_categories(
            variable          = b['variable'],
            group_assignments = {str(k): int(v) for k, v in b['group_assignments'].items()},
        )
        return jsonify(_serialize(result))
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


# ════════════════════════════════════════════════════════════════════
# OUTPUT DATASET
# ════════════════════════════════════════════════════════════════════

@app.route('/api/transform', methods=['POST'])
def api_transform():
    _require_engine()
    b       = request.get_json()
    out_ds  = b.get('output_dataset', 'woe_output')
    metrics = b.get('metrics', ['woe', 'group', 'label'])
    try:
        df_out   = _engine.transform(_df, variables=b.get('variables'), metrics=metrics)
        dataiku.Dataset(out_ds).write_with_schema(df_out)
        new_cols = [c for c in df_out.columns if c.startswith('opt_')]
        return jsonify(status='ok', output_dataset=out_ds,
                       new_columns=new_cols, n_rows=len(df_out))
    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500


# ════════════════════════════════════════════════════════════════════
# EXPORT ROUTES
# ════════════════════════════════════════════════════════════════════

@app.route('/api/export/bundle')
def api_export_bundle():
    _require_engine()
    try:
        bundle  = _engine.build_scoring_bundle(name=_dsname or 'bundle')
        return Response(bundle.to_json(), mimetype='application/json',
                        headers={'Content-Disposition': 'attachment; filename=bundle.json'})
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/export/python')
def api_export_python():
    _require_engine()
    try:
        bundle = _engine.build_scoring_bundle()
        return Response(bundle.to_python_module(), mimetype='text/plain',
                        headers={'Content-Disposition': 'attachment; filename=scorer.py'})
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/export/sql')
def api_export_sql():
    _require_engine()
    dialect = request.args.get('dialect', 'standard')
    table   = request.args.get('source_table', 'input_table')
    try:
        bundle = _engine.build_scoring_bundle()
        return Response(bundle.to_sql(source_table=table, dialect=dialect),
                        mimetype='text/plain',
                        headers={'Content-Disposition': 'attachment; filename=transform.sql'})
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/export/scorecard')
def api_export_scorecard():
    _require_engine()
    pdo = float(request.args.get('pdo', 20))
    bs  = float(request.args.get('base_score', 600))
    try:
        bundle = _engine.build_scoring_bundle()
        csv    = bundle.to_scorecard_table(pdo=pdo, base_score=bs).to_csv(index=False)
        return Response(csv, mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment; filename=scorecard_table.csv'})
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/preview/python')
def api_preview_python():
    _require_engine()
    var = request.args.get('variable')
    try:
        art = _engine.get_scoring_artifact(var)
        return jsonify(code=art.to_python())
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/preview/sql')
def api_preview_sql():
    _require_engine()
    var     = request.args.get('variable')
    dialect = request.args.get('dialect', 'standard')
    try:
        art = _engine.get_scoring_artifact(var)
        return jsonify(sql=art.to_sql(dialect=dialect))
    except Exception as e:
        return jsonify(error=str(e)), 500


# ════════════════════════════════════════════════════════════════════
# REAL-TIME SCORING
# ════════════════════════════════════════════════════════════════════

@app.route('/api/score_record', methods=['POST'])
def api_score_record():
    _require_engine()
    b = request.get_json()
    try:
        bundle = _engine.build_scoring_bundle()
        result = bundle.score_record(b.get('record', {}))
        return jsonify(output=result)
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route('/api/iv_summary')
def api_iv_summary():
    _require_engine()
    try:
        return jsonify(summary=_engine.get_iv_summary().to_dict(orient='records'))
    except Exception as e:
        return jsonify(error=str(e)), 500


# ════════════════════════════════════════════════════════════════════
# SERIALISER
# ════════════════════════════════════════════════════════════════════

def _serialize(r) -> dict:
    return dict(
        variable            = r.variable,
        dtype               = r.dtype,
        iv                  = round(r.iv, 4),
        iv_interpretation   = interpret_iv(r.iv),
        gini                = round(r.gini, 4),
        ks                  = round(r.ks, 4),
        is_monotonic        = r.is_monotonic,
        monotonic_direction = r.monotonic_direction,
        metric_used         = r.metric_used,
        cutoffs             = [round(c, 6) for c in (r.cutoffs or [])],
        cat_groups          = r.cat_groups,
        bins = [dict(
            label           = b.label,
            group           = b.group,
            lower           = b.lower,
            upper           = b.upper,
            categories      = b.categories,
            count           = b.count,
            event_count     = int(b.event_count),
            non_event_count = int(b.non_event_count),
            event_rate      = round(b.event_rate, 4),
            woe             = round(b.woe, 4),
            iv_contribution = round(b.iv_contribution, 4),
        ) for b in r.bins],
    )

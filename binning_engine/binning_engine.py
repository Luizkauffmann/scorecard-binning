"""
binning_engine.py  (v3)
=======================
Optimal WOE binning engine — numerical + categorical.

New in v3:
  - ScoringArtifact: portable, self-contained scoring logic per variable
  - Export formats: JSON schema, Python if/else code, SQL CASE WHEN, Scorecard points table
  - ScoringBundle: pack ALL variables into one JSON/Python module for deployment
  - Real-time scoring: score_record(record_dict) → dict of transformed values
  - Dataiku recipe helper: apply_bundle_to_dataset()
"""

import json, textwrap, datetime
import numpy as np
import pandas as pd
import warnings
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Literal

warnings.filterwarnings("ignore")


# ═══════════════════════════════════════════════════════════════════════════
# Data structures
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class BinStats:
    label: str
    group: int
    lower: Optional[float]
    upper: Optional[float]
    categories: Optional[List[str]]
    count: int
    event_count: float
    non_event_count: float
    event_rate: float
    woe: float
    iv_contribution: float


@dataclass
class BinningResult:
    variable: str
    dtype: str                      # "numerical" | "categorical"
    bins: List[BinStats]
    cutoffs: List[float]
    cat_groups: Optional[List[List[str]]]
    iv: float
    gini: float
    ks: float
    is_monotonic: bool
    monotonic_direction: Optional[str]
    metric_used: str
    optb_object: Any = field(default=None, repr=False)

    def summary(self) -> pd.DataFrame:
        rows = [{'Label': b.label, 'Group': b.group, 'Count': b.count,
                 'Events': int(b.event_count), 'Non-Events': int(b.non_event_count),
                 'Event Rate': round(b.event_rate,4), 'WOE': round(b.woe,4),
                 'IV Contribution': round(b.iv_contribution,4)}
                for b in self.bins]
        df = pd.DataFrame(rows)
        print(f"\n{'='*60}\nVariable: {self.variable} [{self.dtype}]")
        print(f"IV={self.iv:.4f} Gini={self.gini:.4f} KS={self.ks:.4f} Bins={len(self.bins)}")
        if self.dtype == 'numerical':
            print(f"Cutoffs: {[round(c,4) for c in self.cutoffs]}")
        print('='*60)
        print(df.to_string(index=False))
        return df


# ═══════════════════════════════════════════════════════════════════════════
# ScoringArtifact — portable scoring logic for one variable
# ═══════════════════════════════════════════════════════════════════════════

class ScoringArtifact:
    """
    Self-contained scoring rules for a single variable.
    Can export to JSON, Python, SQL, or scorecard table.
    Can score a single value directly.

    Attributes
    ----------
    variable      : original column name  (e.g. "AGE")
    output_col    : output column name    (e.g. "opt_AGE")
    dtype         : "numerical" | "categorical"
    rules         : ordered list of rule dicts (see below)
    missing_woe   : WOE to assign for null / unknown values
    missing_group : group to assign for null / unknown values

    Rule dict schema
    ----------------
    Numerical:
      {"group": 1, "lower": 18.0, "upper": 30.0,
       "lower_inclusive": False, "upper_inclusive": True,
       "woe": -0.512, "event_rate": 0.12, "count": 340}

    Categorical:
      {"group": 2, "categories": ["Unemployed", "Student"],
       "woe": 0.823, "event_rate": 0.34, "count": 210}
    """

    def __init__(self, result: BinningResult, output_col: Optional[str] = None):
        self.variable    = result.variable
        self.output_col  = output_col or f"opt_{result.variable}"
        self.dtype       = result.dtype
        self.iv          = result.iv
        self.gini        = result.gini
        self.ks          = result.ks
        self.missing_woe   = 0.0
        self.missing_group = 0
        self.rules = self._build_rules(result)

    def _build_rules(self, result: BinningResult) -> List[dict]:
        rules = []
        if result.dtype == 'numerical':
            for b in result.bins:
                rules.append({
                    "group":            b.group,
                    "lower":            b.lower,
                    "upper":            b.upper,
                    "lower_inclusive":  False,   # (lower, upper]
                    "upper_inclusive":  True,
                    "woe":              round(b.woe, 6),
                    "event_rate":       round(b.event_rate, 6),
                    "label":            b.label,
                    "count":            b.count,
                })
        else:
            for b in result.bins:
                rules.append({
                    "group":       b.group,
                    "categories":  b.categories or [],
                    "woe":         round(b.woe, 6),
                    "event_rate":  round(b.event_rate, 6),
                    "label":       b.label,
                    "count":       b.count,
                })
        return rules

    # ----------------------------------------------------------------
    # Score a single value → (group, woe)
    # ----------------------------------------------------------------

    def score_value(self, value) -> dict:
        """
        Score a single raw value.
        Returns {"group": int, "woe": float, "label": str}
        """
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return {"group": self.missing_group, "woe": self.missing_woe, "label": "Missing"}

        if self.dtype == 'numerical':
            fval = float(value)
            for rule in self.rules:
                if fval <= rule['upper']:
                    return {"group": rule['group'], "woe": rule['woe'], "label": rule['label']}
            last = self.rules[-1]
            return {"group": last['group'], "woe": last['woe'], "label": last['label']}

        else:  # categorical
            sval = str(value)
            for rule in self.rules:
                if sval in rule['categories']:
                    return {"group": rule['group'], "woe": rule['woe'], "label": rule['label']}
            return {"group": self.missing_group, "woe": self.missing_woe, "label": "Unknown"}

    # ----------------------------------------------------------------
    # Export: JSON schema
    # ----------------------------------------------------------------

    def to_json(self) -> dict:
        """
        Machine-readable JSON schema.
        Can be consumed by any language / REST API.
        """
        return {
            "variable":     self.variable,
            "output_col":   self.output_col,
            "dtype":        self.dtype,
            "iv":           self.iv,
            "gini":         self.gini,
            "ks":           self.ks,
            "missing_woe":  self.missing_woe,
            "missing_group": self.missing_group,
            "rules":        self.rules,
        }

    @classmethod
    def from_json(cls, d: dict) -> "ScoringArtifact":
        """Reconstruct from a JSON dict (for loading a saved bundle)."""
        obj = object.__new__(cls)
        obj.variable     = d['variable']
        obj.output_col   = d['output_col']
        obj.dtype        = d['dtype']
        obj.iv           = d.get('iv', 0)
        obj.gini         = d.get('gini', 0)
        obj.ks           = d.get('ks', 0)
        obj.missing_woe  = d.get('missing_woe', 0.0)
        obj.missing_group= d.get('missing_group', 0)
        obj.rules        = d['rules']
        return obj

    # ----------------------------------------------------------------
    # Export: Python if/else function
    # ----------------------------------------------------------------

    def to_python(self) -> str:
        """
        Generate a standalone Python function with explicit if/else logic.
        Zero external dependencies — copy-paste into any codebase.
        """
        var_safe = self.variable.lower().replace(' ', '_').replace('-', '_')
        out_safe = self.output_col.lower().replace(' ', '_').replace('-', '_')
        lines = []
        lines.append(f"def score_{var_safe}(value):")
        lines.append(f'    """')
        lines.append(f'    Score {self.variable} → ({self.output_col}_group, {self.output_col}_woe)')
        lines.append(f'    IV={self.iv:.4f}  Gini={self.gini:.4f}  KS={self.ks:.4f}')
        lines.append(f'    Returns: dict with "group" (int) and "woe" (float)')
        lines.append(f'    """')
        lines.append(f'    import math')
        lines.append(f'    try:')
        lines.append(f'        is_null = value is None or (isinstance(value, float) and math.isnan(value))')
        lines.append(f'    except Exception:')
        lines.append(f'        is_null = False')
        lines.append(f'    if is_null:')
        lines.append(f'        return {{"group": {self.missing_group}, "woe": {self.missing_woe}, "label": "Missing"}}')
        lines.append(f'')

        if self.dtype == 'numerical':
            lines.append(f'    value = float(value)')
            for i, rule in enumerate(self.rules):
                lo  = rule['lower']
                hi  = rule['upper']
                grp = rule['group']
                woe = rule['woe']
                lbl = rule['label'].replace("'", "\\'")
                if i == 0:
                    lines.append(f"    if value <= {hi}:")
                elif i == len(self.rules) - 1:
                    lines.append(f"    else:  # >= {lo}")
                else:
                    lines.append(f"    elif value <= {hi}:")
                lines.append(f'        return {{"group": {grp}, "woe": {woe}, "label": "{lbl}"}}')
        else:
            lines.append(f'    value = str(value)')
            for i, rule in enumerate(self.rules):
                cats = rule['categories']
                grp  = rule['group']
                woe  = rule['woe']
                lbl  = rule['label'].replace("'", "\\'")
                cats_repr = repr(set(cats))
                if i == 0:
                    lines.append(f"    if value in {cats_repr}:")
                elif i == len(self.rules) - 1:
                    lines.append(f"    elif value in {cats_repr}:")
                else:
                    lines.append(f"    elif value in {cats_repr}:")
                lines.append(f'        return {{"group": {grp}, "woe": {woe}, "label": "{lbl}"}}')
            lines.append(f'    else:')
            lines.append(f'        return {{"group": {self.missing_group}, "woe": {self.missing_woe}, "label": "Unknown"}}')

        return '\n'.join(lines)

    # ----------------------------------------------------------------
    # Export: SQL CASE WHEN
    # ----------------------------------------------------------------

    def to_sql(self, dialect: Literal['standard', 'spark', 'bigquery'] = 'standard') -> str:
        """
        Generate a SQL CASE WHEN expression.
        Returns two expressions: one for the group, one for the WOE.
        """
        col = self.variable
        out = self.output_col
        null_fn = {
            'standard': f'{col} IS NULL',
            'spark':     f'{col} IS NULL',
            'bigquery':  f'{col} IS NULL',
        }[dialect]

        def case_block(metric: Literal['group', 'woe']) -> str:
            lines = [f"CASE"]
            lines.append(f"    WHEN {null_fn} THEN "
                         + (str(self.missing_group) if metric == 'group' else str(self.missing_woe)))
            if self.dtype == 'numerical':
                for i, rule in enumerate(self.rules):
                    val = rule['group'] if metric == 'group' else rule['woe']
                    if i == 0:
                        lines.append(f"    WHEN {col} <= {rule['upper']} THEN {val}")
                    elif i == len(self.rules) - 1:
                        lines.append(f"    ELSE {val}  -- >= {rule['lower']}")
                    else:
                        lines.append(f"    WHEN {col} <= {rule['upper']} THEN {val}")
            else:
                for rule in self.rules:
                    val  = rule['group'] if metric == 'group' else rule['woe']
                    cats = ', '.join(f"'{c}'" for c in rule['categories'])
                    lines.append(f"    WHEN {col} IN ({cats}) THEN {val}")
                lines.append(f"    ELSE "
                             + (str(self.missing_group) if metric == 'group' else str(self.missing_woe))
                             + "  -- Unknown / new category")
            lines.append(f"END AS {out}_{'group' if metric=='group' else 'woe'}")
            return '\n'.join(lines)

        header = f"-- {self.variable} → {self.output_col}  |  IV={self.iv:.4f}"
        return header + '\n' + case_block('group') + ',\n' + case_block('woe')

    # ----------------------------------------------------------------
    # Export: scorecard points table
    # ----------------------------------------------------------------

    def to_scorecard_table(self, pdo: float = 20, base_score: float = 600,
                           base_odds: float = 1/19) -> pd.DataFrame:
        """
        Classic scorecard points per bin.
        Points = -(WOE * Factor) + Offset/n_variables
        Factor = PDO / ln(2)
        """
        factor = pdo / np.log(2)
        rows = []
        for rule in self.rules:
            woe   = rule['woe']
            pts   = round(-woe * factor, 1)
            row = {
                'Variable':   self.variable,
                'Group':      rule['group'],
                'Label':      rule['label'],
                'WOE':        round(woe, 4),
                'Points':     pts,
                'Event Rate': round(rule.get('event_rate', 0), 4),
                'Count':      rule.get('count', ''),
            }
            if self.dtype == 'categorical':
                row['Categories'] = ' | '.join(rule.get('categories', []))
            rows.append(row)
        rows.append({
            'Variable': self.variable, 'Group': 0, 'Label': 'Missing / Unknown',
            'WOE': self.missing_woe, 'Points': round(-self.missing_woe * factor, 1),
            'Event Rate': '', 'Count': ''
        })
        return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════
# ScoringBundle — all variables packed together for deployment
# ═══════════════════════════════════════════════════════════════════════════

class ScoringBundle:
    """
    Packs all ScoringArtifacts into a single deployable unit.

    Can export:
      - bundle.json          : full machine-readable schema
      - scorer.py            : self-contained Python module, no deps
      - transform.sql        : full SQL SELECT with all CASE WHENs
      - scorecard_table.csv  : all variables' points
    """

    def __init__(self, artifacts: Dict[str, ScoringArtifact],
                 name: str = "binning_bundle",
                 description: str = ""):
        self.artifacts   = artifacts
        self.name        = name
        self.description = description
        self.created_at  = datetime.datetime.utcnow().isoformat() + 'Z'

    # ----------------------------------------------------------------
    # Score one record (real-time API use case)
    # ----------------------------------------------------------------

    def score_record(self, record: dict) -> dict:
        """
        Score a single record dict.

        Input:  {"AGE": 34, "INCOME": 52000, "JOB_TYPE": "Employed", ...}
        Output: {"opt_AGE_group": 2, "opt_AGE_woe": -0.512,
                 "opt_INCOME_group": 3, "opt_INCOME_woe": 0.231, ...}
        """
        out = {}
        for var, artifact in self.artifacts.items():
            raw_val = record.get(var)
            result  = artifact.score_value(raw_val)
            out[f"{artifact.output_col}_group"] = result['group']
            out[f"{artifact.output_col}_woe"]   = result['woe']
            out[f"{artifact.output_col}_label"]  = result['label']
        return out

    def score_dataframe(self, df: pd.DataFrame,
                        metrics: List[str] = ['woe', 'group']) -> pd.DataFrame:
        """Score an entire DataFrame. Adds opt_* columns."""
        out = df.copy()
        for var, artifact in self.artifacts.items():
            if var not in out.columns:
                continue
            results = out[var].apply(artifact.score_value)
            if 'group' in metrics:
                out[f"{artifact.output_col}_group"] = results.apply(lambda r: r['group'])
            if 'woe' in metrics:
                out[f"{artifact.output_col}_woe"] = results.apply(lambda r: r['woe'])
            if 'label' in metrics:
                out[f"{artifact.output_col}_label"] = results.apply(lambda r: r['label'])
        return out

    # ----------------------------------------------------------------
    # Export: JSON bundle
    # ----------------------------------------------------------------

    def to_json(self, indent: int = 2) -> str:
        bundle = {
            "name":        self.name,
            "description": self.description,
            "created_at":  self.created_at,
            "version":     "3.0",
            "variables":   {var: art.to_json() for var, art in self.artifacts.items()},
        }
        return json.dumps(bundle, indent=indent)

    def save_json(self, path: str):
        with open(path, 'w') as f:
            f.write(self.to_json())
        print(f"Bundle saved → {path}")

    @classmethod
    def load_json(cls, path: str) -> "ScoringBundle":
        with open(path) as f:
            d = json.load(f)
        artifacts = {var: ScoringArtifact.from_json(art)
                     for var, art in d['variables'].items()}
        return cls(artifacts=artifacts, name=d.get('name',''),
                   description=d.get('description',''))

    # ----------------------------------------------------------------
    # Export: self-contained Python scorer module
    # ----------------------------------------------------------------

    def to_python_module(self) -> str:
        """
        Generate a complete Python module with:
          - One scoring function per variable
          - A score_record(record) function
          - A score_dataframe(df) function
          - No external dependencies (stdlib only)
        """
        lines = []
        lines.append('"""')
        lines.append(f'scorer.py — auto-generated by BinningEngine v3')
        lines.append(f'Bundle: {self.name}')
        lines.append(f'Generated: {self.created_at}')
        lines.append(f'Variables: {", ".join(self.artifacts.keys())}')
        lines.append('"""')
        lines.append('')
        lines.append('import math')
        lines.append('')

        # Per-variable functions
        for art in self.artifacts.values():
            lines.append(art.to_python())
            lines.append('')

        # score_record
        lines.append('def score_record(record):')
        lines.append('    """')
        lines.append('    Score a single record dict.')
        lines.append('    Input:  {"AGE": 34, "INCOME": 52000, ...}')
        lines.append('    Output: {"opt_AGE_group": 2, "opt_AGE_woe": -0.512, ...}')
        lines.append('    """')
        lines.append('    out = {}')
        for art in self.artifacts.values():
            fn  = art.variable.lower().replace(' ','_').replace('-','_')
            col = art.output_col
            lines.append(f'    r = score_{fn}(record.get("{art.variable}"))')
            lines.append(f'    out["{col}_group"] = r["group"]')
            lines.append(f'    out["{col}_woe"]   = r["woe"]')
            lines.append(f'    out["{col}_label"]  = r["label"]')
        lines.append('    return out')
        lines.append('')

        # score_dataframe (pandas optional)
        lines.append('def score_dataframe(df):')
        lines.append('    """')
        lines.append('    Score a pandas DataFrame. Returns df with opt_* columns added.')
        lines.append('    Requires pandas.')
        lines.append('    """')
        for art in self.artifacts.values():
            fn  = art.variable.lower().replace(' ','_').replace('-','_')
            col = art.output_col
            lines.append(f'    if "{art.variable}" in df.columns:')
            lines.append(f'        _r = df["{art.variable}"].apply(score_{fn})')
            lines.append(f'        df["{col}_group"] = _r.apply(lambda x: x["group"])')
            lines.append(f'        df["{col}_woe"]   = _r.apply(lambda x: x["woe"])')
            lines.append(f'        df["{col}_label"]  = _r.apply(lambda x: x["label"])')
        lines.append('    return df')
        lines.append('')

        # IV summary as a comment block
        lines.append('# ── IV Summary ──────────────────────────────────────────────')
        for art in self.artifacts.values():
            lines.append(f'# {art.variable:<25} IV={art.iv:.4f}  Gini={art.gini:.4f}  KS={art.ks:.4f}')
        lines.append('# ─────────────────────────────────────────────────────────────')

        return '\n'.join(lines)

    def save_python(self, path: str = 'scorer.py'):
        with open(path, 'w') as f:
            f.write(self.to_python_module())
        print(f"Python scorer saved → {path}")

    # ----------------------------------------------------------------
    # Export: SQL
    # ----------------------------------------------------------------

    def to_sql(self, source_table: str = 'input_table',
               dialect: Literal['standard', 'spark', 'bigquery'] = 'standard') -> str:
        """
        Full SELECT statement with all CASE WHEN transformations.
        Copy-paste into a Dataiku SQL recipe, BigQuery, Spark SQL, etc.
        """
        lines = [f"-- Auto-generated by BinningEngine v3  ({self.created_at})",
                 f"-- Bundle: {self.name}", ""]
        lines.append(f"SELECT")
        lines.append(f"    *,  -- original columns")
        lines.append(f"")
        case_blocks = []
        for art in self.artifacts.values():
            case_blocks.append(
                textwrap.indent(art.to_sql(dialect=dialect), '    ')
            )
        lines.append(',\n\n'.join(case_blocks))
        lines.append(f"\nFROM {source_table}")
        return '\n'.join(lines)

    def save_sql(self, path: str = 'transform.sql',
                 source_table: str = 'input_table',
                 dialect: str = 'standard'):
        with open(path, 'w') as f:
            f.write(self.to_sql(source_table=source_table, dialect=dialect))
        print(f"SQL saved → {path}")

    # ----------------------------------------------------------------
    # Export: scorecard points table
    # ----------------------------------------------------------------

    def to_scorecard_table(self, pdo: float = 20,
                           base_score: float = 600) -> pd.DataFrame:
        frames = [art.to_scorecard_table(pdo=pdo, base_score=base_score)
                  for art in self.artifacts.values()]
        return pd.concat(frames, ignore_index=True)

    def save_scorecard_table(self, path: str = 'scorecard_table.csv',
                             pdo: float = 20, base_score: float = 600):
        df = self.to_scorecard_table(pdo=pdo, base_score=base_score)
        df.to_csv(path, index=False)
        print(f"Scorecard table saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# BinningEngine
# ═══════════════════════════════════════════════════════════════════════════

class BinningEngine:
    def __init__(self, df: pd.DataFrame, target_col: str):
        if target_col not in df.columns:
            raise ValueError(f"Target column '{target_col}' not found.")
        self.df = df.copy()
        self.target_col = target_col
        self._results: Dict[str, BinningResult] = {}

    # ------------------------------------------------------------------
    # Public: fit
    # ------------------------------------------------------------------

    def fit(self, variable, dtype=None, max_bins=10, monotonic='none',
            metric='iv', min_bin_size=0.05, cat_cutoff=0.05):
        if variable not in self.df.columns:
            raise ValueError(f"'{variable}' not in dataframe.")
        if dtype is None:
            dtype = self._detect_dtype(variable)
        x, y = self._get_xy(variable, as_str=(dtype == 'categorical'))
        if dtype == 'numerical':
            result = self._fit_numerical(variable, x, y, max_bins, monotonic, metric, min_bin_size)
        else:
            result = self._fit_categorical(variable, x, y, max_bins, metric, min_bin_size, cat_cutoff)
        self._results[variable] = result
        return result

    def fit_all(self, variables=None, categorical_variables=None,
                max_bins=10, monotonic='none', metric='iv'):
        if variables is None:
            variables = [c for c in self.df.columns if c != self.target_col]
        cat_set = set(categorical_variables or [])
        rows = []
        for v in variables:
            dtype = 'categorical' if v in cat_set else self._detect_dtype(v)
            try:
                r = self.fit(v, dtype=dtype, max_bins=max_bins,
                             monotonic=monotonic, metric=metric)
                rows.append({'Variable': v, 'Type': r.dtype,
                             'IV': round(r.iv,4), 'IV Interpretation': interpret_iv(r.iv),
                             'Gini': round(r.gini,4), 'KS': round(r.ks,4),
                             'Bins': len(r.bins), 'Monotonic': r.is_monotonic})
            except Exception as e:
                print(f"  [skip] {v}: {e}")
        return pd.DataFrame(rows).sort_values('IV', ascending=False).reset_index(drop=True)

    def adjust_cutoffs(self, variable, new_cutoffs):
        self._check_fitted(variable)
        prev = self._results[variable]
        if prev.dtype != 'numerical':
            raise ValueError(f"{variable} is categorical.")
        x, y = self._get_xy(variable)
        result = self._fit_numerical_with_splits(variable, x, y, sorted(new_cutoffs), prev.metric_used)
        self._results[variable] = result
        return result

    def merge_categories(self, variable, group_assignments: Dict[str, int]):
        self._check_fitted(variable)
        if self._results[variable].dtype != 'categorical':
            raise ValueError(f"{variable} is numerical.")
        x, y = self._get_xy(variable, as_str=True)
        groups_dict = {}
        for cat, gid in group_assignments.items():
            groups_dict.setdefault(int(gid), []).append(str(cat))
        cat_groups = [groups_dict[k] for k in sorted(groups_dict.keys())]
        result = self._fit_categorical_with_groups(variable, x, y, cat_groups,
                                                   self._results[variable].metric_used)
        self._results[variable] = result
        return result

    # ------------------------------------------------------------------
    # Public: transform
    # ------------------------------------------------------------------

    def transform(self, df, variables=None, metrics=None):
        if metrics is None:
            metrics = ['woe', 'group', 'label']
        out = df.copy()
        for v in (variables or list(self._results.keys())):
            if v not in self._results:
                continue
            art = ScoringArtifact(self._results[v])
            results = out[v].apply(art.score_value)
            if 'woe'   in metrics: out[f'opt_{v}_woe']   = results.apply(lambda r: r['woe'])
            if 'group' in metrics: out[f'opt_{v}_group']  = results.apply(lambda r: r['group'])
            if 'label' in metrics: out[f'opt_{v}_label']  = results.apply(lambda r: r['label'])
        return out

    def build_output_dataset(self, variables=None, metrics=None):
        return self.transform(self.df, variables=variables, metrics=metrics)

    # ------------------------------------------------------------------
    # Public: scoring artifacts
    # ------------------------------------------------------------------

    def get_scoring_artifact(self, variable: str,
                             output_col: Optional[str] = None) -> ScoringArtifact:
        """Get the scoring artifact for one variable."""
        self._check_fitted(variable)
        return ScoringArtifact(self._results[variable], output_col=output_col)

    def build_scoring_bundle(self, variables: Optional[List[str]] = None,
                              name: str = "binning_bundle",
                              description: str = "") -> ScoringBundle:
        """
        Build a ScoringBundle from all (or specified) fitted variables.
        This is the main export for deployment.
        """
        vars_to_bundle = variables or list(self._results.keys())
        artifacts = {}
        for v in vars_to_bundle:
            if v in self._results:
                artifacts[v] = ScoringArtifact(self._results[v])
        return ScoringBundle(artifacts=artifacts, name=name, description=description)

    # ------------------------------------------------------------------
    # Public: config persistence (for webapp round-trip)
    # ------------------------------------------------------------------

    def export_config(self) -> dict:
        config = {}
        for v, r in self._results.items():
            config[v] = {'dtype': r.dtype, 'metric': r.metric_used,
                         'cutoffs': r.cutoffs if r.dtype=='numerical' else [],
                         'cat_groups': r.cat_groups if r.dtype=='categorical' else None}
        return config

    def import_config(self, config: dict):
        for variable, cfg in config.items():
            if variable not in self.df.columns:
                continue
            x, y = self._get_xy(variable, as_str=(cfg['dtype']=='categorical'))
            if cfg['dtype'] == 'numerical':
                result = self._fit_numerical_with_splits(variable, x, y, cfg['cutoffs'], cfg['metric'])
            else:
                result = self._fit_categorical_with_groups(variable, x, y, cfg['cat_groups'], cfg['metric'])
            self._results[variable] = result

    def get_result(self, variable):
        return self._results.get(variable)

    def get_iv_summary(self) -> pd.DataFrame:
        rows = [{'Variable': v, 'Type': r.dtype, 'IV': round(r.iv,4),
                 'IV Interpretation': interpret_iv(r.iv), 'Gini': round(r.gini,4),
                 'KS': round(r.ks,4), 'Bins': len(r.bins)}
                for v, r in self._results.items()]
        return pd.DataFrame(rows).sort_values('IV', ascending=False).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Internal: numerical fitting
    # ------------------------------------------------------------------

    def _fit_numerical(self, variable, x, y, max_bins, monotonic, metric, min_bin_size):
        mono_map = {'none': None, 'increasing': 'ascending',
                    'decreasing': 'descending', 'auto': 'auto'}
        try:
            from optbinning import OptimalBinning
            optb = OptimalBinning(name=variable, dtype='numerical', solver='mip',
                                  max_n_bins=max_bins, min_bin_size=min_bin_size,
                                  monotonic_trend=mono_map.get(monotonic))
            optb.fit(x, y)
            splits = sorted(optb.splits.tolist())
            return self._build_numerical(variable, x, y, splits, metric, optb)
        except ImportError:
            splits = self._greedy_cutoffs(x, y, max_bins)
            if monotonic != 'none':
                splits = self._enforce_mono(x, y, splits, monotonic)
            return self._build_numerical(variable, x, y, splits, metric, None)

    def _fit_numerical_with_splits(self, variable, x, y, splits, metric):
        try:
            from optbinning import OptimalBinning
            optb = OptimalBinning(name=variable, dtype='numerical', solver='cp',
                                  user_splits=np.array(splits))
            optb.fit(x, y)
            return self._build_numerical(variable, x, y, splits, metric, optb)
        except Exception:
            return self._build_numerical(variable, x, y, splits, metric, None)

    # ------------------------------------------------------------------
    # Internal: categorical fitting
    # ------------------------------------------------------------------

    def _fit_categorical(self, variable, x, y, max_bins, metric, min_bin_size, cat_cutoff):
        try:
            from optbinning import OptimalBinning
            optb = OptimalBinning(name=variable, dtype='categorical', solver='mip',
                                  max_n_bins=max_bins, min_bin_size=min_bin_size,
                                  cat_cutoff=cat_cutoff)
            optb.fit(x, y)
            cat_groups = self._extract_cat_groups(optb, x)
            return self._build_categorical(variable, x, y, cat_groups, metric, optb)
        except ImportError:
            cat_groups = self._greedy_cat_groups(x, y, max_bins)
            return self._build_categorical(variable, x, y, cat_groups, metric, None)

    def _fit_categorical_with_groups(self, variable, x, y, cat_groups, metric):
        try:
            from optbinning import OptimalBinning
            optb = OptimalBinning(name=variable, dtype='categorical', solver='mip',
                                  user_splits=[np.array(g) for g in cat_groups])
            optb.fit(x, y)
            return self._build_categorical(variable, x, y, cat_groups, metric, optb)
        except Exception:
            return self._build_categorical(variable, x, y, cat_groups, metric, None)

    # ------------------------------------------------------------------
    # Internal: stat builders
    # ------------------------------------------------------------------

    def _build_numerical(self, variable, x, y, splits, metric, optb):
        te = max(y.sum(), 1); tne = max(len(y)-te, 1)
        bounds = [-np.inf] + sorted(splits) + [np.inf]
        bins = []
        for i in range(len(bounds)-1):
            lo, hi = bounds[i], bounds[i+1]
            mask = (x > lo) & (x <= hi)
            ev = float(y[mask].sum()); ne = float(mask.sum())-ev
            er = ev/mask.sum() if mask.sum()>0 else 0
            woe = np.log(max(ev/te,1e-10)/max(ne/tne,1e-10))
            iv_c = (ev/te - ne/tne)*woe
            lo_r = float(x.min()) if np.isinf(lo) else lo
            hi_r = float(x.max()) if np.isinf(hi) else hi
            fmt = lambda v: f"{v:,.2f}" if abs(v)<1e5 else f"{v:,.0f}"
            n = len(bounds)-2
            lbl = (f"< {fmt(hi_r)}" if i==0 else f">= {fmt(lo_r)}" if i==n else f"{fmt(lo_r)} – {fmt(hi_r)}")
            bins.append(BinStats(label=lbl, group=i+1, lower=lo_r, upper=hi_r,
                                 categories=None, count=int(mask.sum()),
                                 event_count=ev, non_event_count=ne,
                                 event_rate=er, woe=woe, iv_contribution=iv_c))
        iv,ks,gini = self._global_stats(bins)
        woes = [b.woe for b in bins]
        is_inc = all(woes[i]>=woes[i-1]-1e-4 for i in range(1,len(woes)))
        is_dec = all(woes[i]<=woes[i-1]+1e-4 for i in range(1,len(woes)))
        return BinningResult(variable=variable, dtype='numerical', bins=bins,
                             cutoffs=sorted(splits), cat_groups=None,
                             iv=iv, gini=gini, ks=ks,
                             is_monotonic=(is_inc or is_dec),
                             monotonic_direction=('flat' if is_inc and is_dec else
                                                  'increasing' if is_inc else
                                                  'decreasing' if is_dec else None),
                             metric_used=metric, optb_object=optb)

    def _build_categorical(self, variable, x, y, cat_groups, metric, optb):
        te = max(y.sum(),1); tne = max(len(y)-te,1)
        bins = []
        for i, group_cats in enumerate(cat_groups):
            cats_set = set(str(c) for c in group_cats)
            mask = np.array([str(v) in cats_set for v in x])
            ev = float(y[mask].sum()); ne = float(mask.sum())-ev
            er = ev/mask.sum() if mask.sum()>0 else 0
            woe = np.log(max(ev/te,1e-10)/max(ne/tne,1e-10))
            iv_c = (ev/te-ne/tne)*woe
            lbl = ' | '.join(sorted(str(c) for c in group_cats))
            bins.append(BinStats(label=lbl, group=i+1, lower=None, upper=None,
                                 categories=[str(c) for c in group_cats],
                                 count=int(mask.sum()), event_count=ev, non_event_count=ne,
                                 event_rate=er, woe=woe, iv_contribution=iv_c))
        iv,ks,gini = self._global_stats(bins)
        woes = [b.woe for b in bins]
        is_mono = (all(woes[i]>=woes[i-1]-1e-4 for i in range(1,len(woes))) or
                   all(woes[i]<=woes[i-1]+1e-4 for i in range(1,len(woes))))
        return BinningResult(variable=variable, dtype='categorical', bins=bins,
                             cutoffs=[], cat_groups=cat_groups,
                             iv=iv, gini=gini, ks=ks, is_monotonic=is_mono,
                             monotonic_direction=None, metric_used=metric, optb_object=optb)

    # ------------------------------------------------------------------
    # Internal: stats + helpers
    # ------------------------------------------------------------------

    def _global_stats(self, bins):
        iv = sum(b.iv_contribution for b in bins)
        te = sum(b.event_count for b in bins) or 1
        tne = sum(b.non_event_count for b in bins) or 1
        ce=cne=ks=0.0
        for b in bins:
            ce+=b.event_count/te; cne+=b.non_event_count/tne
            ks=max(ks,abs(ce-cne))
        return iv, ks, 2*ks

    def _detect_dtype(self, variable):
        col = self.df[variable]
        return 'numerical' if (pd.api.types.is_numeric_dtype(col) and col.nunique()>10) else 'categorical'

    def _get_xy(self, variable, as_str=False):
        x=self.df[variable]; y=self.df[self.target_col]
        mask=x.notna()&y.notna()
        xc=x[mask].values
        return (xc.astype(str) if as_str else xc.astype(float)), y[mask].values.astype(int)

    def _check_fitted(self, variable):
        if variable not in self._results:
            raise ValueError(f"'{variable}' not fitted yet.")

    def _extract_cat_groups(self, optb, x):
        try:
            splits = optb.splits
            if splits is None or len(splits)==0:
                return [[str(c)] for c in np.unique(x)]
            return [[str(c) for c in grp] for grp in splits]
        except Exception:
            return [[str(c)] for c in np.unique(x)]

    def _greedy_cat_groups(self, x, y, max_bins):
        cats = np.unique(x)
        er = {c: y[x==c].mean() if (x==c).sum()>0 else 0 for c in cats}
        groups = np.array_split(sorted(cats, key=lambda c: er[c]),
                                min(max_bins, len(cats)))
        return [[str(c) for c in g] for g in groups if len(g)>0]

    def _greedy_cutoffs(self, x, y, max_bins):
        q = np.linspace(0,100,max_bins+1)[1:-1]
        return sorted(np.unique(np.percentile(x,q)).tolist())

    def _enforce_mono(self, x, y, cuts, direction):
        cuts = list(cuts)
        for _ in range(50):
            woes = self._quick_woe(x, y, cuts)
            vi = None
            for i in range(1,len(woes)):
                ok=(woes[i]>=woes[i-1]-1e-4 if direction=='increasing' else woes[i]<=woes[i-1]+1e-4)
                if not ok and cuts: vi=i; break
            if vi is None or not cuts: break
            cuts.pop(vi-1)
        return cuts

    def _quick_woe(self, x, y, cuts):
        te=max(y.sum(),1); tne=max(len(y)-te,1)
        bounds=[-np.inf]+sorted(cuts)+[np.inf]
        return [np.log(max(y[(x>bounds[i])&(x<=bounds[i+1])].sum()/te,1e-10)/
                       max(((x>bounds[i])&(x<=bounds[i+1])).sum()-y[(x>bounds[i])&(x<=bounds[i+1])].sum(),1)/tne)
                for i in range(len(bounds)-1)]


# ═══════════════════════════════════════════════════════════════════════════
# Dataiku recipe helper
# ═══════════════════════════════════════════════════════════════════════════

def apply_bundle_to_dataset(bundle_path: str,
                             input_dataset,     # dataiku.Dataset
                             output_dataset,    # dataiku.Dataset
                             metrics=None,
                             chunksize: int = 50_000):
    """
    Apply a ScoringBundle to a Dataiku dataset in chunks.
    Suitable for large datasets without loading everything into memory.

    Usage in a Dataiku recipe:
        import dataiku
        from binning_engine import apply_bundle_to_dataset
        apply_bundle_to_dataset(
            bundle_path = '/path/to/bundle.json',
            input_dataset  = dataiku.Dataset('my_input'),
            output_dataset = dataiku.Dataset('my_output'),
        )
    """
    if metrics is None:
        metrics = ['woe', 'group']
    bundle = ScoringBundle.load_json(bundle_path)
    first = True
    for chunk in input_dataset.iter_dataframes(chunksize=chunksize):
        out = bundle.score_dataframe(chunk, metrics=metrics)
        if first:
            output_dataset.write_schema_from_dataframe(out)
            writer = output_dataset.get_writer()
            first = False
        writer.write_dataframe(out)
    writer.close()


# ═══════════════════════════════════════════════════════════════════════════
# IV interpretation
# ═══════════════════════════════════════════════════════════════════════════

IV_BANDS = [(0,.02,'Useless'),(0.02,.1,'Weak'),(0.1,.3,'Medium'),
            (0.3,.5,'Strong'),(0.5,9999,'Very strong — check for leakage')]

def interpret_iv(iv):
    for lo,hi,lbl in IV_BANDS:
        if lo<=iv<hi: return lbl
    return 'Unknown'


# ═══════════════════════════════════════════════════════════════════════════
# Demo
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    np.random.seed(42)
    n = 3000
    age    = np.clip(np.random.normal(42,12,n), 18, 80)
    income = np.clip(np.random.normal(65000,35000,n), 10000, 300000)
    debt_r = np.clip(np.random.normal(0.38,0.22,n), 0, 1)
    jobs   = np.random.choice(['Employed','Self-employed','Unemployed','Retired','Student'],
                               n, p=[.4,.2,.1,.2,.1])
    jmap   = {'Employed':-.5,'Self-employed':-.2,'Unemployed':1.2,'Retired':-.3,'Student':.8}
    logit  = -2+.03*age-.00001*income+2*debt_r+np.array([jmap[j] for j in jobs])+.3*np.random.randn(n)
    target = (np.random.rand(n)<1/(1+np.exp(-logit))).astype(int)
    df = pd.DataFrame({'AGE':age,'INCOME':income,'DEBT_RATIO':debt_r,'JOB_TYPE':jobs,'target':target})

    engine = BinningEngine(df, 'target')
    engine.fit('AGE', max_bins=5, monotonic='increasing', metric='iv')
    engine.fit('INCOME', max_bins=5, metric='iv')
    engine.fit('JOB_TYPE', dtype='categorical', metric='iv')

    # ── Build scoring bundle ──────────────────────────────────────────
    bundle = engine.build_scoring_bundle(name="demo_scorecard")

    print("\n══ JSON bundle (first 30 lines) ══")
    print('\n'.join(bundle.to_json().split('\n')[:30]))

    print("\n══ Python scorer (AGE function) ══")
    age_art = engine.get_scoring_artifact('AGE')
    print(age_art.to_python())

    print("\n══ SQL CASE WHEN (AGE) ══")
    print(age_art.to_sql(dialect='standard'))

    print("\n══ Real-time scoring — single record ══")
    record = {"AGE": 34, "INCOME": 52000, "JOB_TYPE": "Unemployed"}
    print(bundle.score_record(record))

    print("\n══ Scorecard points table ══")
    print(bundle.to_scorecard_table(pdo=20, base_score=600).to_string(index=False))

    print("\n══ Output dataset (first 3 rows) ══")
    out_df = bundle.score_dataframe(df.head(3))
    opt_cols = [c for c in out_df.columns if c.startswith('opt_')]
    print(out_df[['AGE','JOB_TYPE']+opt_cols])

    # Save all artifacts
    bundle.save_json('/tmp/bundle.json')
    bundle.save_python('/tmp/scorer.py')
    bundle.save_sql('/tmp/transform.sql')
    bundle.save_scorecard_table('/tmp/scorecard_table.csv')
    print("\nAll artifacts saved to /tmp/")

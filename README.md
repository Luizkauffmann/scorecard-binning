# Scorecard Interactive Binning — Dataiku Webapp Plugin

Interactive WOE/IV optimal binning tool for credit scorecard development, built as a Dataiku Standard Webapp.

## Features
- Numerical variables: draggable cutoff lines, monotonicity constraints
- Categorical variables: drag-and-drop category grouping
- Optimization by IV, Gini, or KS statistic
- Export scoring artifacts: Python scorer, SQL CASE WHEN, JSON bundle, scorecard points table
- Real-time single-record scoring test

## Requirements
- Dataiku DSS 11+
- Python 3.9+
- optbinning >= 0.18.0

## Files
- `backend.py` — Flask API (paste into Dataiku Python tab)
- `binning_engine.py` — Core WOE binning engine (place in python-lib/)
- `body.html` — HTML markup (paste into Dataiku HTML tab)
- `app.js` — JavaScript (paste into Dataiku JS tab)
- `app.css` — Styles (paste into Dataiku CSS tab)

## Author
Luiz Henrique Kauffmann

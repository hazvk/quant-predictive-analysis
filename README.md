# quant-predictive-analysis
Produce analysis on stocks in a scalable way

# Usage

## Setup
```
$ python initialize.py [-y]
```

## Ingestion and curation
```
$ python main.py
```

## Teardown
```
$ python teardown.py [-y]
```

## Reset (combines above 3)
```
$ bash helpers/reset_db_and_seed.sh
```

# TODOs

* Understand metrics and functionality in [helpers/train_and_evaluate_model.ipynb](helpers/train_and_evaluate_model.ipynb)
* Research suitability for ML modelling for daily trends - look into swing trading
    * counts by portfolio achieved using:
    ```
    df1 = duck_db_conn.sql("select ticker, COUNT(*) as cnt from stocks_raw GROUP BY 1").to_df()
    df1.merge(STOCK_PORTFOLIOS_DF, on=["ticker"]).groupby("portfolio").sum("cnt")
    ```
* Investigate if we can get smaller intervals (note: 15 minutes can only be used for 60 days worth of data)
    * also consider creating different ingestion implementations depending on the interval of the data
* Implement ML training and assessment as part of PROD pipeline
* Pick some different tickers
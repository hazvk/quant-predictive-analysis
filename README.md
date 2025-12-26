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

# TODOs

* Understand metrics and functionality in [helpers/train_and_evaluate_model.ipynb](helpers/train_and_evaluate_model.ipynb)
* Research suitability for ML modelling for daily trends
* Investigate if we can get smaller intervals (note: 15 minutes can only be used for 60 days worth of data)
    * also consider creating different ingestion implementations depending on the interval of the data
* Implement ML training and assessment as part of PROD pipeline
* Pick some different tickers - ASX if possible
# **Makeship**

## **Overview**

This private repository is the submission of the Technical Test for the Senior Data Analyst role. In this submission, there are 2 folders. 

- **EDA:** A folder to host notebooks related to Olist data. The first notebook dedicated to perform Exploratory Data Analysis on Olist's sales funnel is currently included. 

- **eCommerce:** A `data.py` is included to `Olist()` class to request datasets from S3 and clean dataset. It also includes `single_seller` function to perform single seller analysis in the Post-Sales stage, easily expandable to include multiple functions.

Datasets Provided:

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/olistbr/brazilian-ecommerce)

[Marketing Funnel by Olist](https://www.kaggle.com/olistbr/marketing-funnel-olist)

## **Setup**
In order to reproduce the result, we will first need `poetry` package:

```
pip install poetry
```

In the repository, install dependencies with the `poetry` package:

```
poetry install
```

The Notebook also includes Prophet, a time series prediction library developed by Facebook. However, we currently are not able to include it in the Poetry package, most likely due to how [Prophet is setup](https://facebook.github.io/prophet/docs/installation.html#python). After performing `poetry install`, user should have `pystan` on the local machine. Simply run:

```
pip install fbprophet
```

If unsuccessful, try install `pystan` again:
```
pip install pystan
```

This should allow user to interact with all code cells within the Jupyter Notebook. 

## Future Development 

Every analysis should be conducted with having the future development in mind. This is the exact reason why we have developed a module to clean the data and perform single seller analysis. Here are some of the direction that we could further develop on:

- Continue to develop `data.py` and other modules to perform custom-built data wrangling and analysis
- Create a Knowledge Base to keep track of growing insights and questions
- Advanced statistical analysis and time series analysis to further examine eCommerce data
- Various machine learning techniques 
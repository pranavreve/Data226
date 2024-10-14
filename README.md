# Stock Price Prediction Analytics System

This repository contains the code and resources for Lab 1: **Building a Stock Price Prediction Analytics System** using **Snowflake** and **Airflow**. The project uses the Alpha Vantage API to fetch daily stock pricing information and implements ETL and machine learning forecasting pipelines as Airflow DAGs.

## Problem Statement

The goal of this lab is to develop a system that:
- Extracts stock price data from the Alpha Vantage API for Apple (AAPL) and NVIDIA (NVDA).
- Transforms and loads the data into a Snowflake table.
- Uses Snowflake's ML Forecasting features to predict stock prices for the next 7 days based on the last 90 days of data.
- Runs both the ETL pipeline and the ML forecasting pipeline as Airflow DAGs.

## System Architecture

![System Architecture Diagram](path_to_your_diagram.png)

The system consists of:
1. **Alpha Vantage API**: Fetches daily stock price data.
2. **Airflow ETL Pipeline**: Extracts, transforms, and loads stock data into Snowflake.
3. **Snowflake ML Forecasting**: Predicts stock prices based on the historical data.
4. **Airflow ML Pipeline**: Runs the ML model and stores predictions in Snowflake.

## Table Structure

The stock data is stored in a Snowflake table with the following structure:

| Field   | Type           | Description                     |
|---------|----------------|---------------------------------|
| date    | TIMESTAMP_NTZ   | The date of the stock data      |
| open    | FLOAT           | Opening price of the stock      |
| high    | FLOAT           | Highest price of the stock      |
| low     | FLOAT           | Lowest price of the stock       |
| close   | FLOAT           | Closing price of the stock      |
| volume  | INT             | Number of shares traded         |
| symbol  | STRING          | Stock symbol (AAPL or NVDA)     |

## Getting Started

### Prerequisites

- **Python 3.8+**
- **Airflow 2.x**
- **Snowflake Account** with necessary privileges
- **Alpha Vantage API Key** (get it from [Alpha Vantage](https://www.alphavantage.co/support/#api-key))
- **Snowflake Hook** setup in Airflow for connection

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/stock-price-prediction.git
   cd stock-price-prediction

-- Creating a view for training data
-- This view selects relevant columns (DATE, CLOSE, SYMBOL) from the raw stock data table.
-- The view will be used as input to the Snowflake ML model for training purposes.
CREATE OR REPLACE VIEW dev.adhoc.stock_data_view AS 
SELECT
    DATE,  -- The date of the stock data
    CLOSE, -- The closing price of the stock on a given date
    SYMBOL -- The stock symbol (AAPL for Apple, NVDA for NVIDIA)
FROM 
    dev.raw_data.stock_data;  -- Source table containing raw stock data

-- Creating and Training Snowflake ML Model
-- This creates a machine learning forecast function using the Snowflake ML engine.
-- It trains the model based on the data in the 'stock_data_view' created above.
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST dev.analytics.predict_stock_price (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'dev.adhoc.stock_data_view'),  -- Reference to the view containing the training data
    SERIES_COLNAME => 'SYMBOL',     -- Stock symbol as the series (i.e., the different stocks being predicted)
    TIMESTAMP_COLNAME => 'DATE',    -- Date column for time series prediction
    TARGET_COLNAME => 'CLOSE',      -- The target column to predict (closing price of the stock)
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }  -- Skip any errors encountered during training (optional)
);

-- Generating Predictions and saving them to a forecast table
-- This runs the forecast for the next 7 days based on the trained ML model.
-- The results are stored in the 'stock_data_forecast' table.
CALL dev.analytics.predict_stock_price!FORECAST(
    FORECASTING_PERIODS => 7,      -- Number of days to forecast (7-day stock prediction)
    CONFIG_OBJECT => {'prediction_interval': 0.95}  -- 95% confidence interval for the predictions
);

-- Store the forecast results into the forecast table.
-- The RESULT_SCAN function retrieves the output of the forecast operation.
LET x := SQLID;  -- SQLID stores the identifier for the result set
CREATE OR REPLACE TABLE dev.adhoc.stock_data_forecast AS 
SELECT * FROM TABLE(RESULT_SCAN(:x));  -- Store the forecast results

-- Creating the final table by combining historical stock data with forecasted data
-- This UNION merges the historical stock data with the forecasted stock prices.
-- The final table includes both actual prices (from the past) and predicted prices (for the next 7 days).
CREATE OR REPLACE TABLE dev.analytics.final_stock_data AS
    SELECT 
        SYMBOL, 
        DATE, 
        CLOSE AS actual,   -- Actual stock prices from the raw data
        NULL AS forecast,  -- Placeholder for forecast values (historical rows)
        NULL AS lower_bound, -- Placeholder for lower bound (historical rows)
        NULL AS upper_bound  -- Placeholder for upper bound (historical rows)
    FROM 
        dev.raw_data.stock_data  -- Select historical data from the raw stock table
    UNION ALL
    SELECT 
        replace(series, '"', '') AS SYMBOL,  -- Stock symbol (remove any quotes from the string)
        ts AS DATE,                          -- Timestamp (date of the forecast)
        NULL AS actual,                      -- Placeholder for actual values (forecasted rows)
        forecast,                            -- Forecasted stock prices
        lower_bound,                         -- Lower bound of the prediction interval
        upper_bound                          -- Upper bound of the prediction interval
    FROM 
        dev.adhoc.stock_data_forecast;  -- Forecast data

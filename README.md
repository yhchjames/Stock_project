# Searching veeery lucky Branch

## Target
Find out the branches who get a big gain after a unnormal big buy.  
e.g. Branch 9661 bought 400 shares of 8044 on 2024-10-23. And right after 10-23, price of 8044 is on the rocket.

## Approch

Info 1 : Download branches trading hist data, and tag the big buy day, which is design as   
            1.  The branch bought more than 18% of the volume  
            or  
            2.  The day that the branch bought double to the 7-day-avg self buy  
Info 2: Define big gain start day  
            1. future 7-day-avg price is 20% higher  
            and  
            2. close price is not 3% higher then past 7-day-avg  

Compare Info1 and Info2, find out the branchs who big buy on the big gain start day.  
A list of some branches got veeery lucky on some tickers. And I will follow them once they big buy those ticker which gave them good success rate.

## Usage
All scripts are in py_scripts, paths need to be changed accordingly.  
$run_daily_brokeranaly.sh is the one-button run.  

## Note:
Branches trading data is from FUBON's website.  
Stock price is from twstock. yfinance gave me some wrong data.

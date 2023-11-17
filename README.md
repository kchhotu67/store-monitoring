# Store Monitoring
For monitoring stores uptime and downtime.

## Demo Video
[click here](https://drive.google.com/file/d/1o1QVin4CtYVMklajlI5zZxZ-YipDC90b/view?usp=sharing)

## Prerequisites
Python 3.x installed on your machine.

## Setup
##### Clone Repository:
```git clone https://github.com/kchhotu67/store-monitoring.git```
##### Change into the project directory:
```cd store-monitoring```
##### Create a virtual environment:
```python -m venv .```
##### Activate virtual enviroment:
```source venv/bin/activate```
##### Install project dependencies:
```pip install -r requirements.txt```
##### Put static csv files inside data folder
    put all timezone csv file as 'timezone.csv'
    put store status entry file as 'store_status.csv'
    put menu hours csv file as 'menu_hours.csv'
##### Run project
```python src/server.py```
## Folder Structure & Script Information
##### src/cron.py
This run cron job at every hour and pull latest data from data source and save data in sql database
##### src/generate.py
This script will get triggered when /trigger_report api get called by the user and generate the store report.
##### src/server.py
API Server for **/trigger_report** and **/get_report**  API ENDPOINT
##### data/
This directory contain sqlite database and static csv files for the project
##### output/
This directory contain store_report csv file.


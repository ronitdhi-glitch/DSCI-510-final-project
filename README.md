# DSCI-510-final-Create a virtual environment for pandas (optional if pandas is not installed in the system)

# Sample Project
The project evaluates how AI and automation may reshape employment, highlighting:
Which occupation categories are most vulnerable
Geographic regions with highest risk
Potential future workforce implications 

# Running Analysis
How to install: the user should ensure that requried python libraries are installed in the system. (pip install pandas matplotlib kaggle)

How to run : The project processes data, filters high-risk occupations, performs state and county analysis, and generates graphs and reports automatically through main.py.

This automatically:

Downloads and filters Kaggle dataset
Generates supplemental job analysis
Produces state-level high-risk job reports
Performs California county automation cost analysis
Generates graphs under reports folder 

# Running Tests
Tests.py contain function which shows function execute successfully or not . Functions are :
run_kaggle_ai_job_loss_analysis()
run_supplemental_filter()
run_state_risk_analysis()
run_california_automation_analysis()

# Data Sources
Kaggle AI Job Trends Dataset
O*NET Database — Task Statements.csv
Frey & Osborne (2017) Automation Risk Research
U.S. Census: Population by County

# Findings
Through this project I am trying to find which jobs are at the risk of automation and will be replaced by AI


# Results
Results show that 2134 jobs are at the risk of automatiom and will be replaced by AI based on the analysis of the Task Statements.csv file. 
Thousands of jobs are identified at high risk based on automation probability
AI impact varies significantly across U.S. states
County-level job loss cost estimation is calculated


# Installation
Pandas for Data processing
Kaggle API for Dataset download
Matplotlib for Chart visualization
Seaborn 
openpyxl
Python 3.10+ 

# Project Structure 
project/
│── main.py
│── kaggle_service.py
│── job_filter.py
│── data_filter.py
│── load_data.py
│── state_analysis.py
│── visualization.py
│── state_risk.py
│── county_risk.py
│── utils.py
│── charts.py
│
├── data/ # input and output CSV files
├── reports/ # generated graphs
└── tests/ # unit tests
└── tests/ # unit tests


Introduction: 
This project analyzes how Artificial Intelligence and automation technologies are impacting the workforce in the United States. The goal is to identify which occupations are most vulnerable to job displacement, evaluate geographic exposure, and estimate economic impact across states specifically the state of California and counties. The analysis combines multiple datasets (Kaggle, O*NET, Census) to: 
Identify high-risk occupations based on automation probability
Perform U.S. state-level and California county-level job-loss analysis
Estimate economic impact of job displacement
Generate automated visualizations and reports
The model predicts workforce vulnerability using occupational task analysis and probability scoring derived from Frey & Osborne (2017).

## Data Sources:
| **Dataset**                               | **Type** | **Format**                      | **Raw Data Size** |
|-------------------------------------------|----------|----------------------------------|--------------------|
| O*NET Database                            | File     | CSV                              | 18798              |
| ai_job_trends.csv                         | API      | HTML response → JSON → CSV       | 10006              |
| county_data_1                             | File     | CSV                              | 67                 |
| Frey & Osborne (2017) Automation Risk     | File     | CSV                              | 703                |


## Analysis

This section explains the type of analysis conducted throughout the project.

Steps:

Data Loading – Import datasets using load_data.py

AI Job Loss Classification – Extract automation risk using job_filter.py

Supplemental Occupation Filtering – Score jobs based on exposure level

State-Level Analysis – Compare high-risk job volumes across U.S. states

County-Level Cost Analysis – Estimate economic loss/impact for California

Visualization – Generate charts & heatmaps using charts.py

Algorithms / Methods

Binary risk classifier: probability > 0.64 = high-risk, probability < 0.64 = low-risk

Impact model: workforce population × risk probability

Task-based clustering: job similarity through task weights

Cost estimation: avg salary x population at risk x average weeks the salary is expected to be paid

Training Inputs / Key Fields

Task importance weight

Job specialization & automation percentage

Workforce population (state & county)

Salary & economic exposure metrics


## Summary of Results:
2,134+ occupations identified at high-risk of automation

AI impact varies significantly across regions especially in those with high concentreation of service sector jobs

Certain CA counties show extremely high projected economic losses for example Los Angeles

Visualizations highlight concentrated exposure in routine and redundant job categories which require low skill and trainiing. 

### How to Run the Project
Run main automation pipeline:

python main.py

This script automatically:
- Downloads and filters Kaggle dataset
- Performs occupational risk classification
- Executes state‑level and county‑level analysis
- Generates plots and Excel reports inside the `reports/` folder


## Screenshots / Visual Output Examples

Below are sample visuals automatically generated in the `reports/` folder:
- High‑risk occupations bar chart
- State with highest and lowest automation risk comparing visualization
- California county‑level automation cost bar chart
- Summary table export in Excel format


##  **Running Tests**
To verify functions execution:
python tests/tests.py

Functions tested:
- `run_kaggle_ai_job_loss_analysis()`
- `run_supplemental_filter()`
- `run_state_risk_analysis()`
- `run_california_automation_analysis()`
---

##  **Project Structure**
```
src/
│── main.py
│── kaggle_service.py
│── job_filter.py
│── load_data.py
│── data_filter.py
│── state_analysis.py
│── state_risk.py
│── county_risk.py
│── utils.py
│── charts.py
│
├── data/      # raw and processed datasets
├── reports/   # generated plots and Excel files
└── tests/     # unit testing scripts







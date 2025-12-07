# DSCI-510 Final Project – AI & Automation Workforce Impact Analysis

This project analyzes how Artificial Intelligence and automation technologies are reshaping the workforce in the United States. The goal is to identify which occupations are most vulnerable to job displacement, evaluate geographic exposure, and estimate economic impact across states and counties.



## **Project Overview**
The analysis combines multiple datasets (Kaggle, O*NET, Census) to:
-> Identify high‑risk occupations based on automation probability.
-> Perform U.S. state‑level and California county‑level job loss analysis.
-> Estimate financial impact of job displacement.
-> Generate automated visualizations and reports.

This model predicts workforce vulnerability using occupational task analysis and probability scoring derived from Frey & Osborne (2017).

## ⚙️ **Running Analysis**
This section explains how to execute the code and reproduce results.

### **Environment Setup**
Create and activate virtual environment (recommended):
```bash
python -m venv env
source env/bin/activate   # Mac/Linux
env\Scripts\activate     # Windows
```

### **Install Required Libraries**
```bash
pip install -r requirements.txt
```
If running manually, make sure the following libraries are installed:

pandas
matplotlib
seaborn
kaggle
openpyxl

### **Set Up Kaggle API Credentials**
You must configure Kaggle API to automatically download datasets:
```
Place kaggle.json file under: ~/.kaggle/   (Mac)
Place kaggle.json under: C:\Users\<username>\.kaggle\ (Windows)
```
Then give permissions:
```bash
chmod 600 ~/.kaggle/kaggle.json
```

### How to Run the Project**
Run main automation pipeline:

python main.py

This script automatically:
- Downloads and filters Kaggle dataset
- Performs occupational risk classification
- Executes state‑level and county‑level analysis
- Generates plots and Excel reports inside the `reports/` folder


## Visual Output Examples

Below are sample visuals automatically generated in the `reports/` folder:
- High‑risk occupations bar chart
- State comparison visualization
- California county‑level automation cost heatmap
- Summary table export in Excel format

(Place sample images into `reports/` and reference here when publishing on GitHub.)


## Analysis / Model & Pipeline Design
This section explains how the analysis was performed and methodology used.

### **Pipeline Steps**
1. Data Loading** – Import datasets using `load_data.py`
2. AI Job Loss Classification** – Extract automation risk using `job_filter.py`
3. Supplemental Occupation Filtering** – Score jobs based on exposure level
4. State‑Level Analysis** – Compare high‑risk job volumes across U.S. states
5. County‑Level Cost Analysis** – Estimate economic loss for California
6. Visualization** – Generate bar charts & risk heat‑maps using `charts.py`

### **Algorithm / Statistical Methods Used**

 Occupation risk labeling : Binary classifier based on automation probability threshold (>0.64 = high risk) 
 Workforce impact : Weighted population + automation score matrix 
 Occupational clustering : Task similarity and dependency structure 
 Cost estimation : AVG salary × population × risk probability 

### **Training Inputs / Dataset Fields**
- Task importance weight
- Job specialization & automation percentage
- Workforce population by state and county
- Salary / economic risk value


##  **Running Tests**
To verify functions execution:
```bash
python tests/tests.py
```
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
```

---

##  **Results Summary**
- **2134 occupations** identified at **high‑risk** of automation
- AI impact varies significantly between regions
- County‑level cost evaluations reveal major economic exposure zones
- Thousands of jobs estimated to be disrupted across the U.S.

## **Data Sources**
- Kaggle – AI Job Trends Dataset
- O*NET — Task Statements.csv
- Frey & Osborne (2017) Automation Research
- U.S. Census: Population statistics by county

---

##  Findings / Research Insight
AI is expected to automate routine and repetitive job categories first. Technical, creative, and strategic jobs show lower risk. The project supports workforce policy planning and educational priorities.

---

##  Future Work
- Predictive forecasting using regression models
- Industry‑level heat‑map + time‑based automation timeline
- Interactive dashboard deployment (Streamlit / PowerBI)

---

##  Author
**Ronit Dhir** — DSCI‑510 Final Project

---

Thank you for reviewing this project!

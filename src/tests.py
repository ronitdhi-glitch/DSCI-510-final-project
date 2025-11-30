import unittest
import os
import pandas as pd


from main import (
    run_kaggle_ai_job_loss_analysis,
    run_supplemental_filter,
    run_state_risk_analysis,
    run_california_automation_analysis,
)

class TestMainFunctions(unittest.TestCase):

    def test_run_kaggle_ai_job_loss_analysis(self):
        """Check if Kaggle AI job loss analysis runs without errors"""
        try:
            run_kaggle_ai_job_loss_analysis()
        except Exception as e:
            self.fail(f"run_kaggle_ai_job_loss_analysis raised an exception: {e}")

    def test_run_supplemental_filter(self):
        """Check if supplemental filter runs without crashing"""
        try:
            run_supplemental_filter()
        except Exception as e:
            self.fail(f"run_supplemental_filter raised an exception: {e}")

    def test_run_state_risk_analysis(self):
        """Check state risk analysis execution"""
        try:
            run_state_risk_analysis()
        except Exception as e:
            self.fail(f"run_state_risk_analysis raised an exception: {e}")

    def test_run_california_automation_analysis(self):
        """Check California automation risk workflow"""
        try:
            run_california_automation_analysis()
        except Exception as e:
            self.fail(f"run_california_automation_analysis raised an exception: {e}")


if __name__ == "_main_":
    unittest.main()
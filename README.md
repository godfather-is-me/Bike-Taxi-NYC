# Comparing Trip Durations of CitiBikes vs Taxis
- Author: Prathyush P Rao

## Research

**Research Goal:** My research goal is to compare trip durations between CitiBikes and Taxis

**Timeline:** The timeline for the research area is July 2020 to April 2022.

## Report

The report for the project which includes introduction, analysis, and recommendations is available in the `report` folder

## Files

To run the pipeline, please visit the `scripts` directory and run the files in order:
1. run `pip install -r requirements.txt` in the command line to download the required packages (preferably in Python 3.9).
2. `main.py`: This calls subsequent scripts such as `download.py`, `clean.py`, and `process.py` to fully download and process data.
3. `modelling.ipynb`: This notebook builds and runs the models on the preprocessed information and also discusses relevant information
4. `testing_graphs.ipynb`: This notebook is used to conduct analysis on the curated data.

Note: As mentioned in the report, some of the graphs have come from data that has not been merged. If you would like to run that, run the files in order:
1. `main.py`: Change the `OVERALL` keyword in file to True
2. `overall_modelling.ipynb`: Similar to that of modelling
3. `overall_testing_graphs.ipynb`: Similar to that of testing graphs

All other ipynb notebooks not mentioned have been you used for testing and explanation purposes, they don't need to be run

---

Note: Required space to run this project is 30GB and around 2-3 hours of processing time. Further updates to come to help memory management.


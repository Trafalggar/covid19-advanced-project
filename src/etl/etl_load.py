#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import pandas as pd

# --- Config ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "covid19.db")

# Find the latest OWID CSV (most recent)
def get_latest_csv():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("owid_covid_data") and f.endswith(".csv")]
    files.sort(reverse=True)
    if not files:
        raise FileNotFoundError("No OWID file found in data/raw/")
    return os.path.join(DATA_DIR, files[0])

def create_schema(conn):
    cur = conn.cursor()

    # Drop previous tables (reset)
    cur.execute("DROP TABLE IF EXISTS covid_stats;")
    cur.execute("DROP TABLE IF EXISTS locations;")

    # Table locations (static country info)
    cur.execute("""
        CREATE TABLE locations (
            iso_code TEXT PRIMARY KEY,
            continent TEXT,
            location TEXT,
            population REAL,
            population_density REAL,
            median_age REAL,
            aged_65_older REAL,
            aged_70_older REAL,
            gdp_per_capita REAL,
            extreme_poverty REAL,
            cardiovasc_death_rate REAL,
            diabetes_prevalence REAL,
            female_smokers REAL,
            male_smokers REAL,
            handwashing_facilities REAL,
            hospital_beds_per_thousand REAL,
            life_expectancy REAL,
            human_development_index REAL
        );
    """)

    # Table covid_stats (time series)
    cur.execute("""
        CREATE TABLE covid_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            iso_code TEXT,
            date TEXT,
            total_cases REAL,
            new_cases REAL,
            new_cases_smoothed REAL,
            total_deaths REAL,
            new_deaths REAL,
            new_deaths_smoothed REAL,
            total_cases_per_million REAL,
            new_cases_per_million REAL,
            new_cases_smoothed_per_million REAL,
            total_deaths_per_million REAL,
            new_deaths_per_million REAL,
            new_deaths_smoothed_per_million REAL,
            reproduction_rate REAL,
            icu_patients REAL,
            icu_patients_per_million REAL,
            hosp_patients REAL,
            hosp_patients_per_million REAL,
            weekly_icu_admissions REAL,
            weekly_icu_admissions_per_million REAL,
            weekly_hosp_admissions REAL,
            weekly_hosp_admissions_per_million REAL,
            total_tests REAL,
            new_tests REAL,
            total_tests_per_thousand REAL,
            new_tests_per_thousand REAL,
            new_tests_smoothed REAL,
            new_tests_smoothed_per_thousand REAL,
            positive_rate REAL,
            tests_per_case REAL,
            tests_units TEXT,
            total_vaccinations REAL,
            people_vaccinated REAL,
            people_fully_vaccinated REAL,
            total_boosters REAL,
            new_vaccinations REAL,
            new_vaccinations_smoothed REAL,
            total_vaccinations_per_hundred REAL,
            people_vaccinated_per_hundred REAL,
            people_fully_vaccinated_per_hundred REAL,
            total_boosters_per_hundred REAL,
            new_vaccinations_smoothed_per_million REAL,
            new_people_vaccinated_smoothed REAL,
            new_people_vaccinated_smoothed_per_hundred REAL,
            stringency_index REAL,
            excess_mortality_cumulative_absolute REAL,
            excess_mortality_cumulative REAL,
            excess_mortality REAL,
            excess_mortality_cumulative_per_million REAL,
            FOREIGN KEY(iso_code) REFERENCES locations(iso_code)
        );
    """)
    conn.commit()

def load_data(conn, csv_path):
    print(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)

    # Keep only rows with iso_code and date
    df = df.dropna(subset=["iso_code", "date"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Extract locations table (one row per country)
    loc_cols = [
        "iso_code", "continent", "location", "population", "population_density",
        "median_age", "aged_65_older", "aged_70_older", "gdp_per_capita",
        "extreme_poverty", "cardiovasc_death_rate", "diabetes_prevalence",
        "female_smokers", "male_smokers", "handwashing_facilities",
        "hospital_beds_per_thousand", "life_expectancy", "human_development_index"
    ]
    df_locations = df[loc_cols].drop_duplicates(subset=["iso_code"])

    # Extract covid_stats table (time series)
    stat_cols = [c for c in df.columns if c not in loc_cols and c not in ["continent", "location", "population"]]
    df_stats = df[stat_cols].copy()

    # Load into SQL
    df_locations.to_sql("locations", conn, if_exists="append", index=False)
    df_stats.to_sql("covid_stats", conn, if_exists="append", index=False)

    print(f"{len(df_locations)} locations inserted into 'locations'")
    print(f"{len(df_stats)} rows inserted into 'covid_stats'")

def quick_test(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM locations;")
    print("Number of locations:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM covid_stats;")
    print("Number of covid_stats rows:", cur.fetchone()[0])

    cur.execute("""
        SELECT l.location, s.date, s.new_cases, s.new_deaths
        FROM covid_stats s
        JOIN locations l ON l.iso_code = s.iso_code
        WHERE l.location = 'France'
        ORDER BY s.date DESC
        LIMIT 5;
    """)
    rows = cur.fetchall()
    print("Latest France sample:", rows)

def main():
    csv_path = get_latest_csv()
    conn = sqlite3.connect(DB_PATH)

    create_schema(conn)
    load_data(conn, csv_path)
    quick_test(conn)

    conn.close()

if __name__ == "__main__":
    main()

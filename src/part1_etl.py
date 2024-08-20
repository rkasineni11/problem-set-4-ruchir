'''
PART 1: ETL
- This code sets up the datasets for Problem Set 4
- NOTE: You will update this code for PART 4: CATEGORICAL PLOTS
'''

import os
import pandas as pd

def create_directories(directories):
    """
    Creates the necessary directories for storing plots and data.

    Args:
        directories (list of str): A list of directory paths to create.
    """
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def extract_transform():
    """
    Extracts and transforms data from arrest records for analysis

    Returns:
        - `pred_universe`: The dataframe containing prediction-related data for individuals
        - `arrest_events`: The dataframe containing arrest event data
        - `charge_counts`: A dataframe with counts of charges aggregated by charge degree
        - `charge_counts_by_offense`: A dataframe with counts of charges aggregated by both charge degree and offense category
        - `merged_df`: The dataframe merged with felony charge data
    """
    # Extracts arrest data CSVs into dataframes
    pred_universe = pd.read_csv('data/universe_lab9.csv')
    arrest_events = pd.read_csv('data/arrest_events_lab9.csv')

    # Creates two additional dataframes using groupbys
    charge_counts = arrest_events.groupby(['charge_degree']).size().reset_index(name='count')
    charge_counts_by_offense = arrest_events.groupby(['charge_degree', 'offense_category']).size().reset_index(name='count')
    
    # Create felony_charge dataframe
    felony_charge = arrest_events.groupby('arrest_id').apply(
        lambda x: pd.Series({'has_felony_charge': (x['charge_degree'] == 'felony').any()})
    ).reset_index()
    
    # Merge felony_charge with pred_universe
    merged_df = pd.merge(pred_universe, felony_charge, on='arrest_id', how='left')
    
    return pred_universe, arrest_events, charge_counts, charge_counts_by_offense, merged_df

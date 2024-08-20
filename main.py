'''
- You will run Problem Set 4 from this .py, so make sure to set things up to return outputs accordingly
- Go through each PART and write code / make updates as necessary to produce all required outputs
- Run main.py before you start
'''

import src.part1_etl as part1
import src.part2_plot_examples as part2
import src.part3_bar_hist as part3
import src.part4_catplot as part4
import src.part5_scatter as part5

def main():
    ##  PART 1: ETL  ##
    # ETL the datasets into dataframes
    directories = ['data/part2_plots', 'data/part3_plots', 'data/part4_plots', 'data/part5_plots']
    part1.create_directories(directories)
    
    pred_universe, arrest_events, charge_counts, charge_counts_by_offense, merged_df = part1.extract_transform()
    
    # Debugging: Check if 'has_felony_charge' exists in merged_df
    print(merged_df.columns)  # Add this line to check columns in merged_df

    ##  PART 2: PLOT EXAMPLES  ##
    part2.seaborn_settings()
    part2.barplots(charge_counts, charge_counts_by_offense)
    part2.cat_plots(charge_counts, pred_universe)
    part2.histograms(pred_universe)
    part2.scatterplot(pred_universe)

    ##  PART 3: BAR PLOTS AND HISTOGRAMS  ##
    part3.barplot_for_fta(pred_universe)
    part3.barplot_fta_hue_by_sex(pred_universe)
    part3.histogram_age_at_arrest(pred_universe)
    part3.histogram_binned(pred_universe)

    ##  PART 4: CATEGORICAL PLOTS  ##
    part4.catplot_felony_prediction(merged_df)  # Ensure this function receives merged_df
    part4.catplot_nonfelony_prediction(merged_df)
    part4.catplot_felony_prediction_with_actual(merged_df)

    ##  PART 5: SCATTERPLOTS  ##
    part5.scatterplot_felony_vs_nonfelony(merged_df)
    part5.scatterplot_felony_prediction_vs_actual(merged_df)

if __name__ == "__main__":
    main()

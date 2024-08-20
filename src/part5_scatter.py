'''
PART 5: SCATTER PLOTS
- Write functions for the tasks below
- Update main() in main.py to generate the plots and print statments when called
- All plots should be output as PNG files to `data/part5_plots`
'''
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Using lmplot, create a scatter plot where the x-axis is the prediction for felony and the y-axis the is prediction for a nonfelony, and hue this by whether the current charge is a felony. 
def scatterplot_felony_vs_nonfelony(merged_df):
    """
    Create scatter plot of felony vs nonfelony predictions, hued by status of felony.

    Args:
        merged_df (df): df containing merged prediction and felony charge data.
    """
    sns.lmplot(data=merged_df, x='prediction_felony', y='prediction_nonfelony', hue='has_felony_charge', fit_reg=False)
    plt.savefig('data/part5_plots/scatterplot_felony_vs_nonfelony.png', bbox_inches='tight')
    plt.clf()
# In a print statement, answer the following question: What can you say about the group of dots on the right side of the plot?
    print("The dots on the right likely represent people who have a higher chance of being rearrested. This means the model is strongly affected by how serious the current charge is when making predictions.")

# 2. Create a scatterplot where the x-axis is prediction for felony rearrest and the y-axis is whether someone was actually rearrested.
def scatterplot_felony_prediction_vs_actual(merged_df):
    """
    Create scatter plot of felony predictions vs actual rearrest outcomes.

    Args:
        merged_df (df): df containing merged prediction and felony charge data.
    """
    sns.lmplot(data=merged_df, x='prediction_felony', y='y_felony', fit_reg=False)
    plt.savefig('data/part5_plots/scatterplot_felony_prediction_vs_actual.png', bbox_inches='tight')
    plt.clf()
 
# In a print statement, answer the following question: Would you say based off of this plot if the model is calibrated or not?
    print("I don't think the model is reliable because it doesn't follow the pattern, and its predictions don't match the actual rearrests.")
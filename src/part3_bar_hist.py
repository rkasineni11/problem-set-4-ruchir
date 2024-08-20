'''
PART 3: BAR PLOTS AND HISTOGRAMS
- Write functions for the tasks below
- Update main() in main.py to generate the plots and print statments when called
- All plots should be output as PNG files to `data/part3_plots`
'''
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Using the pre_universe data frame, create a bar plot for the fta column.
def barplot_for_fta(pred_universe):
    """
    Create bar plot for fta column in pred_unverise df.

    Args:
        pred_universe (df): df containing prediction data.
    """
    sns.countplot(data=pred_universe, x='fta')
    plt.savefig('data/part3_plots/barplot_for_fta.png', bbox_inches='tight')
    plt.clf() 


# 2. Hue the previous barplot by sex
def barplot_fta_hue_by_sex(pred_universe):
    """
    Create bar plot for fta column hued by sex.

    Args:
        pred_universe (df): df containing prediction data.
    """
    sns.countplot(data=pred_universe, x='fta', hue='sex')
    plt.savefig('data/part3_plots/barplot_fta_hue_by_sex.png', bbox_inches='tight')
    plt.clf()


# 3. Plot a histogram of age_at_arrest
def histogram_age_at_arrest(pred_universe):
    """
    Creates histogram of age_at_arrest column in the pred_universe dataframe.

    Args:
        pred_universe (df): df containing prediction data.
    """
    sns.histplot(data=pred_universe, x='age_at_arrest')
    plt.savefig('data/part3_plots/histogram_age_at_arrest.png', bbox_inches='tight')
    plt.clf()


# 4. Plot the same histogram, but create bins that represent the following age groups 
#  - 18 to 21
#  - 21 to 30
#  - 30 to 40
#  - 40 to 100 
def histogram_binned(pred_universe):
    """
    Create histogram of age_at_arrest column with bins for different age groups.

    Args:
        pred_universe (df): df containing prediction data.
    """
    bins = [18, 21, 30, 40, 100]
    sns.histplot(data=pred_universe, x='age_at_arrest', bins=bins)
    plt.savefig('data/part3_plots/histogram_binned.png', bbox_inches='tight')
    plt.clf()
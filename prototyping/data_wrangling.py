import numpy as np
import pandas as pd


class Data_Wrangling():
    """
    Class to represent methods to clean and transform data.

    ...

    Attributes
    ----------
    None

    Methods
    -------
    download_vaccinated_people_info(n=10000):
        Returns data downloaded from a HTTP request to OpenDataSUS API.

    flag_outliers_in_col(df, col='paciente_idade', threshold=2):
        Flag which documents are outliers
        based on z-score bigger than threshold.

    filter_outliers(self, df, outlier):
        Filter outliers based on flag pd.Series.
    """

    def __init__(self):
        pass

    def flag_outliers_in_col(self, df, col='paciente_idade', threshold=2):
        """
        Flag which documents are outliers
        based on z-score bigger than threshold.

            Parameters:
            ----------
                df : pd.DataFrame
                    OpenDataSUS DataFrame

                col : str
                    target column to be used in z-score and outlier computation

                threshold : int
                    threshold to consider a document an outlier

            Returns:
            ----------
                pd.Series
                    True/False pd.Series, true being an outlier
        """
        data = df[col]
        mean = np.mean(data)
        std = np.std(data)
        outlier = []
        for i in data:
            z = (i-mean)/std
            outlier.append(z > threshold)
        outlier = pd.Series(outlier)
        print(f"Number of outliers: {outlier.sum()}")
        return outlier

    def filter_outliers(self, df, outlier):
        """
        Filter outliers based on flag pd.Series.

            Parameters:
            ----------
                df : pd.DataFrame
                    OpenDataSUS DataFrame

                outlier : pd.Series
                    True/False pd.Series, true being an outlier

            Returns:
            ----------
                pd.DataFrame
                    cleaned DataFrame based on outlier
        """
        return df[~outlier].reset_index(drop=True)

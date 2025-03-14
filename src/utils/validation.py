import pandas as pd

def validate_data(source_name, dataframe, schema):
    #TODO: Add validation logic here
    
    
    # placeholder for clean and error rows
    clean_rows = dataframe
    error_rows = pd.DataFrame(columns=dataframe.columns)

    return clean_rows, error_rows
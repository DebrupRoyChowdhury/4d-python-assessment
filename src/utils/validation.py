import pandas as pd

# Function To check enum datatype
def check_enum(val, value_list):
    return (val in value_list)


# Function To check string datatype
def check_string(val):
    return type(val).__name__ == 'str'


# Function To check int datatype
def check_int(val):
    return type(val).__name__ in ['int', 'int64']


# Function To check float datatype
def check_float(val):
    return type(val).__name__ in ['float', 'float64']


# Function To check date datatype
def check_date(val, frmt=None):
    try:
        return type(pd.to_datetime(val, format=frmt)).__name__ == 'Timestamp'
    
    except ValueError:
        return False


# To Get Function As Per DataType's Name
def get_type_func(dt_type):
    match dt_type:
        case "int":
            return check_int
        case "string":
            return check_string
        case "enum":
            return check_enum
        case "date":
            return check_date
        case "float":
            return check_float
        case _:
            raise ValueError(f"Unsupported file type: {dt_type}")


def validate_data(dataframe, schema):
    #TODO: Add validation logic here
    # Variables
    dt_type_dict = {}
    error_indices = []
    cols = dataframe.columns.to_list()

    # Find Erroneous Rows
    for pos, column in enumerate(cols):
        schema_details = schema[pos]
        col_values = dataframe[column]
        col_type = schema_details['type']
        check_function = get_type_func(col_type)

        if ('required' not in schema_details.keys())\
           or\
           (schema_details['required'] != True):
            col_values = dataframe[dataframe[column] == dataframe[column]]\
                                [column]

        if col_type == 'enum':
            check_result = col_values\
                            .apply(lambda x: check_function(x,
                                                    schema_details['values']))
        
        elif col_type == 'date':
            check_result = col_values\
                            .apply(lambda x: check_function(x,
                                                    schema_details['format']))
        
        elif col_type in ['int', 'float', 'int64', 'float64']:
            dt_type = eval(col_type)
            dt_type_dict[column] = dt_type

            for index, value in enumerate(col_values):
                try:
                    dt_type(value)
                
                except ValueError:
                    error_indices.append(index)
            
            col_values = dataframe.drop(index=error_indices)\
                                  [column].astype(dt_type)
            check_result = col_values.apply(check_function)
        
        else:
            check_result = col_values.apply(check_function)

        error_indices.extend(check_result.index[~check_result].to_list())
        error_indices = list(set(error_indices))    
    
    # placeholder for clean and error rows
    clean_rows = dataframe.drop(index=error_indices)
    error_rows = dataframe.loc[error_indices]

    for column, dt_type in dt_type_dict.items():
        clean_rows[column] = clean_rows[column].astype(dt_type)

    return clean_rows, error_rows
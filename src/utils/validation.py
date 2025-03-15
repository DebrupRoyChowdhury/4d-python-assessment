import pandas as pd

# Function To check int datatype
def check_int(val):
    return type(val).__name__ == 'int'


# Function To check string datatype
def check_string(val):
    return type(val).__name__ == 'str'


# Function To check enum datatype
def check_enum(val, value_list):
    return (val in value_list)


# Function To check date datatype
def check_date(val):
    return type(val).__name__ == 'Timestamp'


# Function To check float datatype
def check_float(val):
    return type(val).__name__ == 'float'


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
    error_indices = []
    cols = dataframe.columns.to_list()

    # Find Erroneous Rows
    for pos, column in enumerate(cols):
        schema_details = schema[pos]
        col_values = dataframe[column]
        col_type = schema_details['type']
        check_function = get_type_func(col_type)

        if col_type == 'enum':
            check_result = col_values\
                            .apply(lambda x: check_function(x,
                                                    schema_details['values']))
        else:
            check_result = col_values.apply(check_function)

        error_indices.extend(check_result.index[~check_result].to_list())
        error_indices = list(set(error_indices))    
    
    # placeholder for clean and error rows
    clean_rows = dataframe.drop(index=error_indices)
    error_rows = dataframe.loc[error_indices]

    return clean_rows, error_rows
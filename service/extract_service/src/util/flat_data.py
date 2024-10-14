# Function to flatten nested JSON
def flatten_json(nested_json, parent_key='', sep='_'):
    flat_dict = {}
    for key, value in nested_json.items():
        new_key = f'{parent_key}{sep}{key}' if parent_key else key

        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        else:
            flat_dict[new_key] = value

    return flat_dict
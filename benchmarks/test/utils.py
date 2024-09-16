import os

def increment_file_value(file_path):
    # Check if the file exists
    if os.path.exists(file_path):
        # If the file exists, read its contents
        try:
            with open(file_path, 'r') as file:
                value = int(file.read().strip())  # Read the integer value from the file
        except ValueError:
            print(f"Error: The file {file_path} does not contain a valid integer.")
            return
    else:
        # If the file does not exist, start with value 0
        value = 0

    # Increment the integer by 1
    value += 1

    # Write the new value back to the file
    with open(file_path, 'w') as file:
        file.write(str(value))

    print(f"The file {file_path} now contains the value: {value}")

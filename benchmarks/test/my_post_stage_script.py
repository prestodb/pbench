import sys
from utils import increment_file_value

# Main function to handle the command-line argument
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing <file_path>")
        sys.exit(-1)

    value = -1
    file_path = sys.argv[1]
    if os.path.exists(file_path):
        # If the file exists, read its contents
        try:
            with open(file_path, 'r') as file:
                value = int(file.read().strip())  # Read the integer value from the file
        except:
            pass
    sys.exit(value)

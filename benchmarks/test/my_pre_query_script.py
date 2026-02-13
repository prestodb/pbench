import sys
from utils import increment_file_value

# Main function to handle the command-line argument
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing <file_path>")
        sys.exit(-1)

    file_path = sys.argv[1]
    increment_file_value(file_path)

# Sparse2JSON

Sparse2JSON is a powerful tool designed to identify and convert sparse columns in PostgreSQL databases into a single JSONB column. This tool is particularly useful for datasets, such as those derived from OpenStreetMap (OSM), where many columns contain a high percentage of null values. By consolidating sparse columns into a JSON format, users can optimize their database structure and improve query performance.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)

## Features

- **Identify Sparse Columns**: Automatically detects columns with a significant percentage of NULL values.
- **Convert Sparse Columns**: Migrates sparse data into a new JSONB column, enhancing database efficiency.
- **Transaction Management**: Ensures data integrity through proper transaction handling.
- **Comprehensive Logging**: Tracks the process and errors with detailed logging.
- **Flexible Configuration**: Adjust parameters for minimum row counts (default: 30 row) and sparsity percentages (default: 40% of NULL values).
- **Multi-threading for Performance**: Utilizes multi-threading to parallelize the conversion process for multiple tables, significantly improving performance on larger datasets.
- **Safe Password Handling**: Ensures secure password entry to protect sensitive user information.

## Getting Started

### Prerequisites

To use Sparse2JSON, ensure you have the following installed:

- Python 3.x
- PostgreSQL
- Required Python libraries (install via requirements.txt)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/d3m1s4m/Sparse2JSON
   cd sparse2json
   ```
   
2. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

Sparse2JSON can be executed from the command line to identify and convert sparse columns in PostgreSQL databases. Below are detailed instructions on how to use the tool effectively, including all available command-line options.

### Command-Line Arguments

- `-H` or `--host`: The hostname of the PostgreSQL server (Required).
- `-P` or `--port`: The port of the PostgreSQL server. defaults to **5432** if not specified.
- `-U` or `--user`: The username for database authentication (Required).
- `-D` or `--dbname`: The name of the database to connect to (Required).
- `-T` or `--table`: Specify a particular table to analyze for sparse columns. If omitted, the tool will analyze all tables in the database (Optional).
- `-v` or `--verbose`: Enable verbose output for detailed logging during execution. This provides real-time feedback about the process and any errors encountered.
- `-h` or `--help`: for help and more information.

### Example Usage

1. **Analyze a Specific Table**: To analyze a specific table and convert its sparse columns:
   ```bash
   python main.py -H=<DB_HOST> -D <DB_NAME> -U <DB_USER> -T=<DB_TABLE> -v
   ```
2. **Analyze the Entire Database**: To analyze all tables in the specified database, omit the -T argument:
   ```bash
   python main.py -H=<DB_HOST> -D <DB_NAME> -U <DB_USER> -v
   ```

## Contributing

Contributions are welcome! To contribute to Sparse2JSON, follow these steps:

1. Fork the repository.
2. Create a new feature branch:

   ```bash
   git checkout -b feature/YourFeatureName
   ```

3. Make your changes and commit them:

   ```bash
   git commit -m 'Add a new feature'
   ```

4. Push to the branch:

   ```bash
   git push origin feature/YourFeatureName
   ```

5. Open a pull request.


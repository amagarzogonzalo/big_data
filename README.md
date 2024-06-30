# Data Intensive Systems Project README

## Overview

This repository contains the implementation for the Data Intensive Systems project as part of the MSc in AI program at Utrecht University, developed by Alexo Castro and Alejandro Magarzo.

## Project Structure

- **main.py**: This file is used to execute experiments using configurations specified in `.json` files located in the `Data` directory.
  
- **Data/**: Contains configuration `.json` files that specify different experiments to be conducted. These files are used by `main.py` to define parameters and settings for running experiments.
  
- **data.py**: This script generates datasets required for the experiments defined in the `.json` configuration files.

## How to Use

1. **Running Experiments**:
   - Edit the `.json` configuration files in the `Data` directory to specify different experiment settings.
   - Execute `main.py` to run experiments:
     ```
     python main.py
     ```
   - Results and logs will be generated as specified in the Data folder.

2. **Generating Datasets**:
   - Use `data.py` to generate datasets required for experiments.
   - Modify `data.py` as necessary to customize dataset generation.

## Additional Notes

- Ensure proper documentation and comments are maintained within `main.py`, `data.py`, and `.json` configuration files to facilitate understanding and modification.
- Contact Alexo Castro or Alejandro Magarzo for any project-related inquiries or issues.

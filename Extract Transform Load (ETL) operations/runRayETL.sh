#!/bin/bash

# Array of years to iterate through
years=(1 2 3)

# Run the sortRay.py script for each year
for year in "${years[@]}"
do
    echo "Running sortRay.py with $year year(s)"
    python3 sortRay.py "$year"
done

# Run the transformRay.py script for each year
for year in "${years[@]}"
do
    echo "Running transformRay.py with $year year(s)"
    python3 transformRay.py "$year"
done

# Run the aggregateFilterRay.py script for each year
for year in "${years[@]}"
do
    echo "Running aggregateFilterRay.py with $year year(s)"
    python3 aggregateFilterRay.py "$year"
done

echo "All scripts completed."

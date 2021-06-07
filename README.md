
Basic script to query sacct for job efficiencies for a particular user.

Usage examples:
```
# up to 7 days in the past
./check_efficiency.py s.michele.mesiti 7 
# up to 14 days in the past, discarding the last 7
./check_efficiency.py s.michele.mesiti 14 7
```
The script produces two files, 
in this case `eff_s.michele.mesiti.csv`
and `loweff_s.michele.mesiti.csv`. 
`eff_s.michele.mesiti.csv` contains the list of all jobs 
with some relevant columns,
while `loweff_s.michele.mesiti.csv` contains the list of all jobs 
with efficiency below a given threshold.

*NOTICE*: running jobs will appear in the list 
with 0 efficiency.

Dependencies:
- Python 3.8
- pandas
- tabulate

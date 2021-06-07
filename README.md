
Basic script to query sacct for job efficiencies for a particular user.

Usage examples:
```
# up to 7 days in the past
./check_efficiency.py s.michele.mesiti 7 
# up to 14 days in the past, discarding the last 7
./check_efficiency.py s.michele.mesiti 14 7
```

Dependencies:
- Python 3.8
- pandas
- tabulate

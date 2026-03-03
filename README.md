ipv6 scan analysis using jupyter notebook

run:
```bash
source venv/bin/activate
pip install -r requirements.txt
jupyter notebook
```

- `filter.py` filters out all non-SLAAC addresses.

TODO:
- We want to collect all the non-SLAAC addresses.
- For each address, we want to count how many times each of them is repeated
  - With that, we want to count how many times
- Want to verify that the parallelized workflow is producing the correct result (so we might want a little playground of which the result can be verified through other methods).

Ideas:
- At the filtering step 
  - We can have multiple concurrent processes, and each process a portion of the large file.
  - And then maybe we can write them to the same file, not caring about the order? Although I feel like we should write to separate files and then append them together because I think we do care about locality. 
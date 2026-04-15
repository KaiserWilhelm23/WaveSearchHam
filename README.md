# WaveSearchHam


WAVESEARCH AMATEUR RADIO SERVER - QUICK START


Welcome to WaveSearch — a fast HAM SQL database tool.

NOTE: Map display not working yet.

Online version:
https://wavesearch.kaiser-server.com
(Domain is just a test environment — ignore it)


--------------------------------------------------

1. Install dependency
```python3 -m pip install requests```


--------------------------------------------------

2. Go to WaveSearch_Ham directory
Open terminal in the project folder


--------------------------------------------------

3. Run downloader + server

```python3 ham_download_sql.py```
  (Downloads FCC database and converts .dat → SQL)

```python3 server.py```


--------------------------------------------------

4. Open web UI

http://localhost:8000/search.html


--------------------------------------------------


PROJECT STRUCTURE REQUIRED:
```
WaveSearch_Ham/
 ├── server.py
 ├── ham_download_sql.py
 ├── amateur_calls.sql
 ├── search.html
```

--------------------------------------------------

COMMON ISSUES:

- 404 on page:
  search.html is not in the same folder as server.py

- Empty results:
  SQL database not loaded or wrong dataset name

- Server not responding:
  script not running or wrong port


--------------------------------------------------

NOTES:

- Uses in-memory SQLite database
- Recommended: 200MB+ RAM available

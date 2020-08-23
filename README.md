# monitoring-s
Test workouts for security monitoring company.

## Deploy process

1. Start Elasticsearch on ur local with default port. 
2. Create directory `data` inside of project and put there JSON files.
3. Run `pip install -r requirements.txt`.
4. Run `python create_indexes.py` to create ES indexes.
5. Run `python task_1.py` to check task #1.
6. Run `python task_2.py` to check task #2.
7. Smoke.

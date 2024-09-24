install: 
	pip install --upgrade pip && pip install -r requirements.txt

test: 
	python unit_tests.py

run: 
	python main.py
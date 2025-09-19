.PHONY: venv install lint test run
venv: ; python -m venv .venv
install: ; . .venv/bin/activate && pip install -r requirements.txt
run: ; . .venv/bin/activate && python main.py
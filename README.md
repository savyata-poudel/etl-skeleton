#default command sheet
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env #fill in the correct values for each variables
python -m etl.main --source file --load copy
python -m etl.main --source file --load upsert
python -m etl.main --source api --load upsert
python -m etl.main --source db --load copy


#make command sheet
make venv
make install
cp .env.example .env #fill in the correct values for each variables
python -m etl.main --source file --load copy
python -m etl.main --source file --load upsert
python -m etl.main --source api --load upsert
python -m etl.main --source db --load copy


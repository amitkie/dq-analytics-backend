# Steps to create a virtual env and use it to run the FastAPI App:

## Create a new virtual environment .venv (ignore if already created):
bash
```
python -m venv new_env
```

## Activate the new environment:

### For Windows (Git Bash or Command Prompt):
bash
```
.\new_env\Scripts\activate
```

### For macOS/Linux:
bash
```
source new_env/bin/activate
```

## Install the dependencies:
bash
```
pip install -r requirements.txt
```

## Run the app (e.g., with FastAPI):
bash
```
uvicorn app.backendApi.index:app --reload --port 8001
```

## Deactivate the virtual environment when you're done:
bash
```
deactivate
```
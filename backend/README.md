docker-compose build --no-cache backend

docker-compose up backend

docker-compose restart backend




python -m venv venv

venv\Scripts\activate

pip install -r requirements.txt

pip install geopandas shapely requests

pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

pip install --upgrade torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

python -m uvicorn src.main:app --host 127.0.0.1 --port 8000
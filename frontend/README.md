docker-compose build --no-cache frontend

docker-compose up frontend

docker-compose restart frontend


python -m venv venv

venv\Scripts\activate

pip install -r requirements.txt

python -m streamlit run src/app.py --server.port 8501
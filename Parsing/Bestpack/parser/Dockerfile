FROM docker.adata.kz:443/adata-data-engineers/stack/scraping/python:3.10
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]

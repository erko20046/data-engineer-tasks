FROM docker.adata.kz:443/adata-data-engineers/stack/service/python:3.12
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]

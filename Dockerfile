FROM python:3.9-slim

RUN pip install azure-storage-blob requests
COPY main.py /bin/copy

CMD ["/bin/bash"]

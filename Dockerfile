FROM python:latest
ADD requirements.txt /app/
ADD tests /app/tests
ADD src /app/src
WORKDIR /app/
ENV PYTHONPATH=/app/:$PYTHONPATH
RUN pip install -r requirements.txt
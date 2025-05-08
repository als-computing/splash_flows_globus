FROM prefecthq/prefect:2.20.17-python3.11

WORKDIR /app
COPY ./requirements.txt /tmp/

RUN pip install -U pip &&        pip install -r /tmp/requirements.txt

COPY . /app/
RUN pip install -e .
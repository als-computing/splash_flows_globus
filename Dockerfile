FROM prefecthq/prefect:2.7.9-python3.10

WORKDIR /app
COPY ./requirements.txt /tmp/

RUN pip install -U pip &&        pip install -r /tmp/requirements.txt

COPY . /app/
RUN pip install -e .


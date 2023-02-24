FROM python:3.10-slim

COPY ./requirements.txt ./
COPY ./config.toml ./
COPY ./containers/location ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python", "coordinates.py" ]
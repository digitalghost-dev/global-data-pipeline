FROM python:3.10-slim

COPY ./requirements.txt ./
COPY ./containers/air_quality ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python", "air_quality.py" ]
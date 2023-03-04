FROM python:3.10-slim

COPY ./requirements.txt ./
COPY ./containers/countries ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python", "country_statistics.py" ]
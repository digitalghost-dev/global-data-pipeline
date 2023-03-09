FROM python:3.10-slim

COPY ./requirements.txt ./
COPY ./containers/weather ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python", "city_weather.py" ]
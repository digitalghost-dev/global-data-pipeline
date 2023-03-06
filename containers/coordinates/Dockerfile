FROM python:3.10-slim

COPY ./requirements.txt ./
COPY ./containers/coordinates ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python", "city_coordinates.py" ]
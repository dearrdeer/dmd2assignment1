FROM python:3

WORKDIR /usr/src

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /usr/src


CMD [ "python3", "migrate.py" ]




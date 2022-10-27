FROM python:3.7-alpine3.14

COPY . .

EXPOSE 5555

RUN pip install --no-cache-dir termcolor
ENTRYPOINT ["python", "-u", "node.py"]
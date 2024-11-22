FROM python:3.10

WORKDIR /app

COPY pyproject.toml uv.lock /app/

RUN pip install uv
RUN uv sync --frozen

COPY . /app/

CMD ["uv", "run", "main.py"]

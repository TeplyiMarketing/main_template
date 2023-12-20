FROM python:3.11
LABEL authors="SOneTrue"

RUN python3 -m pip install --upgrade pip
RUN apt-get update && apt-get install -y nano

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /your_client_name

# Копируем файл зависимостей в контейнер
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install -r requirements.txt

# Копируем все файлы вашего проекта в контейнер
COPY . .

# Устанавливаем переменные окружения для подключения к базе данных
ENV PGHOST=${ADDRESS_DB}
ENV PGPORT=${PORT_DB}
ENV PGDATABASE=${NAME_DB}
ENV PGUSER=${USER_DB}
ENV PGPASSWORD=${PASSWORD_DB}

# Устанавливаем временную зону на Europe/Moscow
ENV TZ=Europe/Moscow

CMD [ "python3", "run_program.py" ]


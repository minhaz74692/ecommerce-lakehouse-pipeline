FROM apache/airflow:3.3.0

# Copy requirements file into the image
COPY requirements.txt /requirements.txt

# Switch to airflow user before installing (avoids permission issues at runtime)
USER airflow

# Install pinned providers. --no-cache-dir keeps the image smaller.
RUN pip install --no-cache-dir -r /requirements.txt
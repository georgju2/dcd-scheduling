# dcd-scheduling

# Apache Airflow Setup Guide

This README is a comprehensive guide for setting up Apache Airflow with CeleryExecutor on an Ubuntu machine in an OpenStack environment, utilizing PostgreSQL as the database, and configuring Redis in the Airflow Config file.

## Prerequisites

Before beginning the installation, ensure you have the following:

1. **Ubuntu Server**: A server with Ubuntu OS.
2. **OpenStack Account**: An active OpenStack account with necessary permissions.
3. **Internet Connection**: To download necessary packages and software.

## Step 1: Set Up Ubuntu Server in OpenStack

1. Log in to your OpenStack account and launch an Ubuntu Server instance.
2. Ensure that all the ports required by Apache Airflow are open and properly configured in your security groups.

## Step 2: Update & Upgrade System Packages

SSH into your Ubuntu server and run the following commands to update and upgrade the system packages:

```sh
sudo apt update && sudo apt upgrade -y
```

## Step 3: Install PostgreSQL

1. Install PostgreSQL by running the following command:

```sh
sudo apt-get install postgresql postgresql-contrib -y
```

2. After installation, create a new database and user for Airflow:

```sh
sudo -u postgres psql
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'your_password';
ALTER ROLE airflow_user SET client_encoding TO 'utf8';
ALTER ROLE airflow_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE airflow_user SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
\q
```

## Step 4: Install Apache Airflow

1. Install necessary dependencies:

```sh
sudo apt install -y python3 python3-dev python3-venv build-essential libssl-dev libffi-dev libmysqlclient-dev libxml2-dev libxslt1-dev libssl-dev libffi-dev zlib1g-dev
```

2. Create a new Python virtual environment for Airflow:

```sh
python3 -m venv airflow_venv
```

3. Activate the virtual environment:

```sh
source airflow_venv/bin/activate
```

4. Install Apache Airflow with necessary extras:

```sh
pip install apache-airflow[postgres,crypto,celery,redis]
```

## Step 5: Configure Apache Airflow

1. Initialize the Airflow database:

```sh
airflow db init
```

2. Configure the Airflow configuration file, located at `~/airflow/airflow.cfg`, and modify the following settings:

```sh
[core]
executor = CeleryExecutor

[celery]
result_backend = db+postgresql://airflow_user:your_password@localhost/airflow_db
broker_url = redis://localhost:6379/0

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow_user:your_password@localhost/airflow_db
```

## Step 6: Install & Configure Redis

1. Install Redis:

```sh
sudo apt-get install redis-server -y
```

2. Open the Redis configuration file in a text editor:

```sh
sudo nano /etc/redis/redis.conf
```

3. Find and modify the `supervised` directive:

```sh
supervised systemd
```

4. Restart Redis:

```sh
sudo systemctl restart redis.service
```

## Step 7: Start Apache Airflow

1. Start the Airflow web server and scheduler in separate terminals:

```sh
airflow webserver -p 8080
```

```sh
airflow scheduler
```

2. Access the Airflow UI by navigating to `http://<Your-OpenStack-VM-IP>:8080` in a web browser.

## Final Thoughts

You should now have a running instance of Apache Airflow with CeleryExecutor, configured with PostgreSQL as the database and Redis as the Celery broker. It is important to secure your installation, e.g., by configuring HTTPS, setting up firewalls, etc. 
Refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/) for more information and advanced configurations.

# dcd-scheduling

# Apache Airflow Setup Guide

This README is a guide for setting up Apache Airflow with CeleryExecutor on an Ubuntu machine in an OpenStack environment, utilizing PostgreSQL as the database, and configuring Redis in the Airflow Config file. 

Refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/) for more information and advanced configurations. 

## Prerequisites

Before beginning the installation, ensure you have the following:

1. **Ubuntu Server**: A server with Ubuntu OS.
2. **OpenStack Account**: An active OpenStack account with necessary permissions.

## Step 1: Set Up Ubuntu Server in OpenStack

1. Log in to your OpenStack account and launch an Ubuntu Server instance.
2. Ensure that all the ports required by Apache Airflow are open and properly configured in your security groups.

## Step 2: Update & Upgrade System Packages

SSH into your Ubuntu server with the keyfile and run the following commands to update and upgrade the system packages:

```sh
ssh -o StrictHostKeyChecking=no -i ~/.ssh/dcdKey.pem ubuntu@160.85.252.164
```

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
ALTER ROLE airflow_user SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
\CONNECT airflow_db
\q
```

In order to execute the DAGs, we need to create a few tables that correspond to the DAGs

```sh
CREATE TABLE fact (id VARCHAR PRIMARY KEY, fact VARCHAR);

CREATE TABLE accounts ( id serial PRIMARY KEY, created_on TIMESTAMP NOT NULL,  unit INT );

PostgreSQL provides a “\dt” command to list all the available tables of a database
```

To restart Postgresql
```sh
sudo systemctl restart postgresql
```

For more information on [setting up Postgresql on Ubuntu refer to this link](https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-22-04-quickstart)

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

5. Install Celery Flower

```sh
pip install celery-flower
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
result_backend = db+postgresql+psycopg2://airflow_user:your_password@localhost/airflow_db
broker_url = redis://localhost:6379/0

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow_user:your_password@localhost/airflow_db
```

Create Airflow users

```sh
airflow users create -u admin -f admin -l admin -r admin -e admin@byom.de

airflow users create -u guest -f guest -l guest -r guest -e guest@byom.de
```

See list of airflow users

```sh
airflow users list
```

For more information on [installing and configuring Apache Airflow on Ubuntu please refer to this link](
https://medium.com/international-school-of-ai-data-science/setting-up-apache-airflow-in-ubuntu-324cfcee1427)

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

5. Alternative: To start Redis in daemon mode without protection (only for non prod envs)

```sh
redis-server --protected-mode no --daemonize yes 
```

For more information on [installing REDIS on Ubuntu please refer to this link](https://www.digitalocean.com/community/tutorials/how-to-install-and-secure-redis-on-ubuntu-22-04)

## Step 7: Start Apache Airflow

1. Start the Airflow web server and scheduler in separate terminals:

```sh
airflow webserver -p 8080
```

```sh
airflow scheduler
```

Celery worker can be started on multiple hosts to enable distributed execution.
To monitor celery workers use Celery Flower.

```sh
airflow celery worker

airflow celery flower
```

2. Access the Airflow UI by navigating to `http://<Your-OpenStack-VM-IP>:8080` in a web browser.

## Final Thoughts

You should now have a running instance of Apache Airflow with CeleryExecutor, configured with PostgreSQL as the database and Redis as the Celery broker. 

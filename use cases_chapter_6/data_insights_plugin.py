from datetime import datetime, timedelta
from collections import Counter
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataInsightScheduler:

    # Evaluates the data freshness of the records for a given timespan
    @staticmethod
    def analyze_data_freshness(db_name, timespan):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()

        current_time = datetime.now()
        lookback_time = current_time - timedelta(hours=timespan)
        query = f"""SELECT COUNT(*) FROM {db_name} WHERE last_updated > %s;"""
        cursor.execute(query, [lookback_time])
        result = cursor.fetchone()
        print(f"Result of Data Freshness check: {result} new records found for time period {timespan} hours in {db_name}")
        cursor.close()
        connection.close()
        return result[0] > 0


    # Analyses the rate at which the data is being updated for a given timespan
    @staticmethod
    def analyze_data_change_rate(db_name, timespan):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()

        current_time = datetime.now()
        lookback_time = current_time - timedelta(hours=timespan)
        change_rate_query = f"""
            SELECT COUNT(*) FROM {db_name}
            WHERE last_updated BETWEEN %s AND %s;
            """
        cursor.execute(change_rate_query, [lookback_time, current_time])
        changes_count = cursor.fetchone()[0]

        total_records_query = f"SELECT COUNT(*) FROM {db_name};"
        cursor.execute(total_records_query)
        total_count = cursor.fetchone()[0]

        change_rate_percentage = (changes_count / total_count) * 100
        print(f"Data Change Rate: {change_rate_percentage}% of records were updated in the last {timespan} hours in {db_name}")

        cursor.close()
        connection.close()
        return change_rate_percentage

    # Evaluates if the data change rate is below a given threshold
    @staticmethod
    def is_change_rate_below_threshold(db_name, timespan, threshold):
        change_rate_percentage = DataInsightScheduler.analyze_data_change_rate(db_name, timespan)
        return change_rate_percentage < threshold

    # Finds out periods of low activity for a given timespan and returns the intervals
    @staticmethod
    def find_low_activity_periods(db_name, interval_hours=1, lookback_duration=timedelta(days=365)):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()

        current_time = datetime.now()
        lookback_time = current_time - lookback_duration
        interval_duration = timedelta(hours=interval_hours)
        interval_counts = Counter()

        while lookback_time < current_time:
            interval_end = lookback_time + interval_duration
            activity_query = f"""
                SELECT COUNT(*) FROM {db_name}
                WHERE last_updated BETWEEN %s AND %s;
                """
            cursor.execute(activity_query, [lookback_time, interval_end])
            updates_count = cursor.fetchone()[0]
            interval_counts[lookback_time.strftime("%Y-%m-%d %H:%M:%S")] = updates_count
            lookback_time = interval_end

        cursor.close()
        connection.close()

        # Find the intervals with the lowest activity by reversing the most common list
        activity_periods = interval_counts.most_common()[::-1]  # Reverse to get least common intervals

        # Get top 5 least activity periods
        low_activity_periods = activity_periods[:5]
        
        print(f"Periods with low activity: {low_activity_periods}")
        return low_activity_periods


    # Analyze Query Performance of a database over a given timespan
    @staticmethod
    def analyze_query_performance(db_name, timespan_days=7):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()

        current_time = datetime.now()
        past_time = current_time - timedelta(days=timespan_days)
        performance_data = {}

        # Query to analyze historical query execution times
        performance_query = f"""
            SELECT date_trunc('hour', query_start) AS hour, avg(EXTRACT(EPOCH FROM (query_end - query_start))) AS avg_duration
            FROM pg_stat_activity
            WHERE datname = %s AND query_start BETWEEN %s AND %s
            GROUP BY date_trunc('hour', query_start)
            ORDER BY date_trunc('hour', query_start);
            """
        cursor.execute(performance_query, [db_name, past_time, current_time])
        query_performance = cursor.fetchall()

        cursor.close()
        connection.close()

        # Process the performance data
        for hour, avg_duration in query_performance:
            performance_data[hour.strftime("%Y-%m-%d %H:%M:%S")] = avg_duration

        print(f"Performance Data to forecast task scheduling with DB load insights: {performance_data}")
        return performance_data



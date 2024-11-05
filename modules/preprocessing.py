import pyspark.sql.functions as F
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession, Window
from typing import List, Tuple



def get_true_dtypes(sdf: DataFrame) -> DataFrame:
    '''
    Cast columns in the input DataFrame to the appropriate data types.

    Parameters
    ----------
    sdf : DataFrame
        A Spark DataFrame containing columns 'user_id', 'session_id',
        and 'timestamp' that need to be cast to long type.
    
    Returns
    -------
    DataFrame
        A Spark DataFrame with 'user_id', 'session_id',
        and 'timestamp' columns cast to long type.
    '''
    return (
        sdf
        .withColumn('user_id', F.col('user_id').cast('long'))
        .withColumn('session_id', F.col('session_id').cast('long'))
        .withColumn('timestamp', F.col('timestamp').cast('long'))
    )


def exclude_error_events(sdf: DataFrame) -> DataFrame:
    '''
    Excludes events occurring after an error within each user session.

    Parameters
    ----------
    sdf : DataFrame
        A Spark DataFrame containing at least the columns 'user_id',
        'session_id', 'event_type', and 'timestamp'. The 'event_type'
        column is used to identify error events.
    
    Returns
    -------
    DataFrame
        A Spark DataFrame with rows removed that occur after the first
        error event within each user session.
    '''
    # Define a window partitioned by user_id and session_id, ordered by timestamp
    window_spec = Window.partitionBy('user_id', 'session_id').orderBy('timestamp')
    
    # Create 'is_error' column: 1 if event_type contains 'error', else 0
    sdf = sdf.withColumn(
        'is_error',
        F.when(F.lower(F.col('event_type')).contains('error'), 1).otherwise(0)
    )

    # Calculate cumulative sum of 'is_error' to flag rows after the error
    sdf = (
        sdf
        .withColumn('cum_error', F.sum('is_error').over(window_spec))
        .filter(F.col('cum_error') == 0)
    )
    
    return sdf


def exclude_same_pages(sdf: DataFrame) -> DataFrame:
    '''
    Excludes consecutive events that occur on the same page within
    each user session.

    Parameters
    ----------
    sdf : DataFrame
        A Spark DataFrame containing columns 'user_id', 'session_id',
        'event_page', and 'timestamp'. The 'event_page' column is used
        to determine consecutive page events.
    
    Returns
    -------
    DataFrame
        A Spark DataFrame with consecutive events on the same page removed,
        keeping only transitions between different pages within
        each user session.
    '''
    # Define a window partitioned by user_id and session_id, ordered by timestamp
    window_spec = Window.partitionBy('user_id', 'session_id').orderBy('timestamp')
    
    # Detect changes in 'event_page' to identify route steps
    return (
        sdf
        .withColumn('prev_event_page', F.lag('event_page').over(window_spec))
        .withColumn(
            'page_changed',
            F.when(
                F.isnull('prev_event_page')
                | (F.col('event_page') != F.col('prev_event_page')), 1
            )
            .otherwise(0)
        )
        .filter(F.col('page_changed') == 1)
    )


def collect_route(sdf: DataFrame) -> DataFrame:
    '''
    Collects and counts the unique routes taken by users within sessions.

    Parameters
    ----------
    sdf : DataFrame
        A Spark DataFrame containing columns 'user_id', 'session_id',
        'event_page', and 'timestamp'. The 'event_page' column represents
        the pages visited, and 'timestamp' is used for ordering events
        within each session.
    
    Returns
    -------
    DataFrame
        A Spark DataFrame containing unique routes as a string of page
        transitions and their corresponding counts, indicating the number
        of times each route was followed.
    '''
    # Define a window partitioned by user_id and session_id, ordered by timestamp
    window_spec = Window.partitionBy('user_id', 'session_id').orderBy('timestamp')
    
    routes_grouped_sdf = (
        sdf
        .withColumn('sorted_list', F.collect_list('event_page').over(window_spec))
        .groupBy('user_id', 'session_id')
        .agg(F.max('sorted_list').alias('sorted_pages'))
    )
    
    # Create the route by concatenating the sorted pages
    routes_grouped_sdf = (
        routes_grouped_sdf
        .withColumn(
            'route',
            F.concat_ws('-', 'sorted_pages')
        )
    )
    
    # Count the occurrences of each route
    return routes_grouped_sdf.groupBy('route').count()


def get_top_n_routes_df(sdf: DataFrame, n: int) -> DataFrame:
    '''
    Retrieves the top N routes based on their frequency count.

    Parameters
    ----------
    sdf : DataFrame
        A Spark DataFrame containing routes and their corresponding counts. 
        It should include a 'count' column representing the frequency
        of each route.
    n : int
        The number of top routes to retrieve.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing the top N routes, sorted by
        the 'count' column in descending order.
    '''
    return sdf.orderBy(F.desc('count')).limit(n)


def get_ready_sql_query() -> str:
    '''
    Generates an SQL query to extract the top 10 most common routes taken by
    users based on session data.

    Parameters
    ----------
    None

    Returns
    -------
    str
        A string containing the SQL query. The query performs the following:
        - Filters out events occurring after an error event within each session.
        - Identifies changes in the 'event_page' for each user session.
        - Collects routes for each session by concatenating pages visited.
        - Calculates and retrieves the top 10 most common routes sorted by their count.
    '''
    return """
    WITH base_data AS (
        SELECT
            CAST(user_id AS BIGINT) AS user_id,
            CAST(session_id AS BIGINT) AS session_id,
            CAST(timestamp AS BIGINT) AS timestamp,
            event_type,
            event_page
        FROM clickstream
    ),
    errors AS (
        SELECT
            user_id,
            session_id,
            MIN(timestamp) AS error_timestamp
        FROM base_data
        WHERE LOWER(event_type) LIKE '%error%'
        GROUP BY user_id, session_id
    ),
    valid_data AS (
        SELECT
            bd.user_id,
            bd.session_id,
            bd.timestamp,
            bd.event_page
        FROM base_data bd
        LEFT JOIN errors e
            ON bd.user_id = e.user_id AND bd.session_id = e.session_id
        WHERE e.error_timestamp IS NULL OR bd.timestamp < e.error_timestamp
    ),
    page_changes AS (
        SELECT
            user_id,
            session_id,
            timestamp,
            event_page
        FROM (
            SELECT
                user_id,
                session_id,
                timestamp,
                event_page,
                LAG(event_page) OVER (
                    PARTITION BY user_id, session_id
                    ORDER BY timestamp
                ) AS prev_event_page
            FROM valid_data
        ) tmp
        WHERE event_page != prev_event_page OR prev_event_page IS NULL
    ),
    routes_filtered AS (
        SELECT user_id, session_id, MAX(route) AS route
        FROM (
                SELECT
                    user_id,
                    session_id,
                    CONCAT_WS('-', COLLECT_LIST(event_page) OVER (
                        PARTITION BY user_id, session_id
                        ORDER BY timestamp ASC)
                    ) AS route
                FROM page_changes
            ) routes
        GROUP BY user_id, session_id
    )
    SELECT
        route,
        COUNT(*) AS count
    FROM routes_filtered
    GROUP BY route
    ORDER BY count DESC
    LIMIT 10
    """


def _process_session(events: List[Tuple[int, str, str]]) -> str:
    '''
    Processes events within a user session to generate a route of
    distinct pages visited before encountering an error event.

    Parameters
    ----------
    events : List[Tuple[int, str, str]]
        A list of tuples, where each tuple represents an event
        with the following fields:
        - timestamp (int): The timestamp of the event.
        - event_type (str): The type of event (e.g., "click", "error").
        - event_page (str): The page associated with the event.

    Returns
    -------
    str
        A string representing the route taken by the user, consisting
        of distinct page transitions, concatenated with '-' as a delimiter.
        The route ends before any error event, if one occurs.
    '''
    events = sorted(events, key=lambda x: x[0])  # Sort by timestamp
    route = []
    prev_event_page = None
    error_occurred = False
    
    for timestamp, event_type, event_page in events:
        if 'error' in event_type.lower():
            error_occurred = True
            
            break  # Stop processing further events after an error
            
        if event_page != prev_event_page:
            route.append(event_page)
            prev_event_page = event_page
            
    return '-'.join(route)


def get_routes(rdd: RDD) -> RDD:
    '''
    Extracts user navigation routes from an RDD of clickstream data.

    Parameters
    ----------
    rdd : RDD
        An RDD where each record is a tuple containing the following fields:
        - user_id (str): The unique identifier of the user.
        - session_id (str): The unique identifier of the session.
        - event_type (str): The type of event (e.g., "click", "error").
        - event_page (str): The page associated with the event.
        - timestamp (str): The timestamp of the event as a string.
    
    Returns
    -------
    RDD
        An RDD containing tuples, where each tuple is of the form:
        - route (str): A string representing the sequence of pages visited in a session.
        - count (int): A count of 1 for each route occurrence, useful for aggregating routes.
    '''
    # Create key-value pairs where key is
    # (user_id, session_id) and value is (timestamp, event_type, event_page)
    session_events = rdd.map(lambda x: ((x[0], x[1]), (int(x[4]), x[2], x[3])))
    session_grouped = session_events.groupByKey()  # Group events by session
    
    # Apply the processing function to each session to get the route
    session_routes = session_grouped.mapValues(_process_session).filter(lambda x: x[1] != '')
    
    # Map routes to (route, 1) for counting
    return session_routes.map(lambda x: (x[1], 1))


def get_top_n_routes_rdd(
    rdd: RDD,
    n: int
    ) -> List[Tuple[str, int]]:
    '''
    Retrieves the top N routes from an RDD based on their frequency count.

    Parameters
    ----------
    rdd : RDD
        An RDD containing tuples where each tuple consists of:
        - route (str): A string representing the sequence of pages visited.
        - count (int): The number of times this route was taken.
    n : int
        The number of top routes to retrieve.
    
    Returns
    -------
    List[Tuple[str, int]]
        A list of tuples representing the top N routes and their counts,
        sorted in descending order by count.
    '''
    # Reduce by key to get the count of each route
    route_counts = rdd.reduceByKey(lambda x, y: x + y)
    
    # Get the top 10 routes by count
    return route_counts.takeOrdered(10, key=lambda x: -x[1])

from google.cloud import bigquery


# Create a "Client" object 
client = bigquery.Client()

# construct a reference to the dataset. The project name is "bigquery-public-data", the name of the dataset is "stackoverflow"
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request o get the dataset
dataset = client.get_dataset(dataset_ref)


""" CREATING TABLE 1 = ANSWERS' TABLE """

# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("posts_answers")

# API request
answers_table = client.get_table(answers_table_ref)

# First five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()

""" CREATING TABLE 2 = QUESTIONS' TABLE """

# Construct a reference to the "posts_questions" table
questions_table_ref = dataset_ref.table("posts_questions")

# API request 
questions_table = client.get_table(questions_table_ref)

# First five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()



def expert_finder(topic, client):
    '''
       Inputs:
        topic: A string with the topic of interest -> choosen by the user
        client: A Client object that specifies the connection to the Stack Overflow dataset

    Outputs:
        results: A DataFrame with columns for user_id and number_of_answers. 
    '''
    
    query = """
               SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
               FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
               INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                   ON q.id = a.parent_Id
               WHERE q.tags like @topic_finder
               GROUP BY a.owner_user_id
               ORDER BY number_of_answers DESC
               """

    # Set up the query 
    query_params = [bigquery.ScalarQueryParameter("topic_finder", "STRING", '%' + topic + '%')] # tricky part: handling the %topic% NOT A STRING PROBLEM !!!
    
    # Error if the query scans too much data. Slow and costly.
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**11, query_parameters = query_params)      
    query_job = client.query(query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    df_results = query_job.to_dataframe()

    return df_results



""" TIME TO CALL THE BACKEND FUNCTION. CHOOISE YOUR TOPIC """

experts_df= expert_finder("pandas", client)   ### <--- USER, CHOOSE YOUR TOPIC HERE (replace "pandas" by "python", "adaboost", etc...)

print(experts_df.head(10))
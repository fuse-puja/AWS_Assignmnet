import logging
import json
import pandas as pd
import os
import boto3
import urllib3

import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

url = "https://jsonplaceholder.typicode.com/posts"
http = urllib3.PoolManager()

def lambda_handler(event, context):
    
    # TODO implement
    logger.info(msg="******Lambda initialized*****")
    response = http.request('GET',url)
    
    # Check if the response status is 200 (OK)
    if response.status == 200:
        
        # Decode the response data as JSON
        data = json.loads(response.data.decode('utf-8'))
        
        # Store the JSON data in an S3 bucket    
        s3.put_object(
            Body=response.data,
            Bucket = 'apprentice-training-puja-data-raw-dev', 
            Key='posts.json')
            
        logger.info(msg="Dumped into source s3 successfully")
    
        
        #Transformation codes:
        data = pd.DataFrame(data)
        
        # Rename the 'title' column to 'post_title'
        data = data.rename(columns={'title': 'post_title'})
        
         # Capitalize the first letter of the content in the 'post_title' column
        data['post_title'] = data['post_title'].str.capitalize()
        
        #keep even valued userId only
        filtered_data = data[data['userId'] % 2 == 0]
        
       
        # Convert the filtered data back to JSON
        transformed_data_json = filtered_data.to_json(orient='records', lines=True)
        
        # Store the JSON data of filtered data in an S3 bucket    
        s3.put_object(Body=transformed_data_json, Bucket = 'apprentice-training-puja-data-cleaned-dev', Key='cleaned_data.json')
        logger.info(msg="Dumped into destination s3 successfully")
        
        
        # Insert data into PostgreSQL
        try:
            conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD']
            )
        
            cur = conn.cursor()
            
            
            
            #inserting into database
            for index, row in filtered_data.iterrows():
                cur.execute(
                    '''INSERT INTO etl_trainnig_puja_posts (userId, id, post_title, body
                        ) VALUES (
                       %s, %s, %s, %s) ''',
                    (
                        row['userId'],
                        row['id'],
                        row['post_title'],
                        row['body']
                    )
                )
            
            conn.commit()

        except Exception as e:
            print("Error:", e)
            conn.rollback()

        finally:
            if conn:
                conn.close()

        
        return {
            'statusCode': 200,
                'body': json.dumps('Data fetched, transformed, and stored successfully!')
            }
                
    else:
        logger.error(f"Error: HTTP status code {response.status}")
        
        return {
            'statusCode': 500,
            'body': json.dumps('Error fetching data from the API')
        }
        
    
   
    
    
    
    


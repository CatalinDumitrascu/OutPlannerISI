# Module Imports
import os
b=os.path.dirname(os.path.abspath(__file__))
os.chdir(b)

import mariadb
import sys
import logging
import requests
import base64
import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(filename = "snd.log", level=logging.INFO, format= '%(levelname)s %(asctime)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S')


QUERY = 'INSERT INTO unprocessed_data_no_full(district,town,name,address,rooms,price,link,soup) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'

def get_recommendations(metadata, indices, title, cosine_sim):
    # Get the index of the movie that matches the title
    idx = indices[title]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Get the scores of the 10 most similar movies
    sim_scores = sim_scores[1:11]

    # Get the movie indices
    movie_indices = [i[0] for i in sim_scores]

    # Return the top 10 most similar movies
    return metadata['name'].iloc[movie_indices]


if __name__ == "__main__": 
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="admin",
            password="admin",
            host="127.0.0.1",
            port=3306,
            database="masterisi"

        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM unprocessed_data")

    unproc_data = cursor.fetchall()
    proc_data = []

    unproc_df = pd.DataFrame(unproc_data)
 

    for row in unproc_data:
        soup = row["district"] + ' ' + str.lower(row["town"])
        address = ''.join(str.lower(row['address'].replace(" ", "").replace(".", "").replace(",", "")))
        rooms = row["rooms"][:2].replace(" ", "")
        price = row["price"][:3].replace(" ", "")
        if not price.isdigit():
            price = "150"
        soup += ' ' + address + ' ' + rooms + ' ' + price
        row["soup"] = soup.replace("/", "").replace("\n", "")
        proc_data.append(row)

    proc_df = pd.DataFrame(proc_data)
    print(proc_df[['soup']].head(2))

    count = CountVectorizer(stop_words='english')
    count_matrix = count.fit_transform(proc_df['soup'])

    cosine_sim = cosine_similarity(count_matrix, count_matrix)
    
    proc_df = proc_df.reset_index()
    indices = pd.Series(proc_df.index, index=proc_df['name'])

    reccs = get_recommendations(proc_df, indices, "Vila Balasa", cosine_sim)

    print(reccs)

        
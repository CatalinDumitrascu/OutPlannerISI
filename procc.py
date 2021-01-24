# Module Imports
import os
b=os.path.dirname(os.path.abspath(__file__))
os.chdir(b)

import mariadb
import sys
import logging
import requests
import base64
import unidecode
import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import linear_kernel
from sklearn.feature_extraction.text import TfidfVectorizer



logging.basicConfig(filename = "snd.log", level=logging.INFO, format= '%(levelname)s %(asctime)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S')


QUERY = 'INSERT INTO unprocessed_data_no_full(district,town,name,address,rooms,price,link,soup) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'

def get_recommendations(metadata, indices, title, cosine_sim):

    idx = indices[title]
    # Get the pairwsie similarity scores of all accs with that acc
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the accs based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:30]
    movie_indices = [i[0] for i in sim_scores]

    # Return the top 10 most similar accs
    return metadata.iloc[movie_indices]


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
        
        rooms = unidecode.unidecode(row["rooms"].replace(" ", ""))
        rooms = rooms.split("in")[0].split("si")[0]
        
        price = unidecode.unidecode(row["price"].replace(" ", "").replace("/", "").replace(".", ""))
        if not price.endswith("dubla"):
            price = "150leinoaptecamdubla"
        soup += ' ' + address + ' ' + rooms + ' ' + price
        row["soup"] = soup.replace("/", "").replace("\n", "")
        proc_data.append(row)

    proc_df = pd.DataFrame(proc_data)
    print(proc_df[['soup']].head(5))

    count = CountVectorizer(stop_words='english')
    count_matrix = count.fit_transform(proc_df['soup'])

    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(proc_df['soup'])

    cosine_sim_count = cosine_similarity(count_matrix, count_matrix)
    cosine_sim_tfidf = linear_kernel(tfidf_matrix, tfidf_matrix)

    proc_df = proc_df.reset_index()
    indices = pd.Series(proc_df.index, index=proc_df['name'])

    while True:
        name = input("Name of accomodation: ")
        type_of_method = input("Method used (tfidf/count): ")
        print(name, type_of_method)
        reccs = None
        try:
            if (type_of_method == "tfidf"):
                reccs = get_recommendations(proc_df, indices, name, cosine_sim_tfidf)
                print(reccs[['district', 'town', 'name', 'rooms', 'price']])
            elif (type_of_method == "count"):
                reccs = get_recommendations(proc_df, indices, name, cosine_sim_count)
                print(reccs[['district', 'town', 'name', 'rooms', 'price']])
            else:
                print("Wrong method input. Try again")
        except Exception as e:
            print("Error while trying to get reccs. Retrying. Error: " + str(e))

        print("Finished")

#Vila Balasa
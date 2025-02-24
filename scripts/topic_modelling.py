from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from google import genai
import re
import io


def topic:

    client = genai.Client(api_key="AIzaSyBh2xX2LL6pCu-C0uY6gHITH535vglwaOk")

    connection_string = "<YOUR_CONNECTION_STRING>"

    container_name = "<YOUR_CONTAINER_NAME>"
    blob_name = "new_reviews.csv"
    download_path = "new_reviews.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    with open(download_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    df = pd.read_csv(download_path)

    df=df[["Clothing ID","Review Text"]].rename(columns={"Clothing ID":"id" , "Review Text" : "review"})

    question = "give me list. give me just the topic names of supporting_category and opposing_category. give me a dictionary. just return two python arrays                   one with supporting_category and another with opposing_category. For example supporting_category = ['Comfort', 'Texture'] opposing_category =                 ['Cost',                 'Durability']"

    df_words=pd.DataFrame(columns=["id","sentiment","supporting_words","opposing_words"])

    question2 = "this is the review feedback text does it prodive positive or negative sentiment overall.Just answer in one word" 


    for i in range(len(df)):
        #print(i)

        response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=df["review"].iloc[i] + "       "+question)

        #print(response.text)

        response2 = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=df["review"].iloc[i] + "       "+question2)

        #print(response2.text)

        pattern = r"supporting_category = \[(.*?)\]"
        pattern2 = r"opposing_category = \[(.*?)\]"

        matches = re.search(pattern, response.text)
        matches2 = re.search(pattern2, response.text)

        temp=matches.group(1)
        temp2=matches2.group(1)

        temp=[i.strip().strip("'") for i in temp.split(",")]
        temp2=[i.strip().strip("'") for i in temp2.split(",")]

        df_words.loc[len(df_words)]= {"id":df["id"].iloc[i],"sentiment":response2.text.split()[0],"supporting_words":temp,"opposing_words":temp2}



    blob_client = container_client.get_blob_client("df_words.csv")

    try:
        # Try to download the existing blob
        existing_blob = blob_client.download_blob()
        existing_data = existing_blob.readall().decode('utf-8')

        # Read the existing blob content into a DataFrame
        existing_df = pd.read_csv(io.StringIO(existing_data))

        # Combine the existing data with the new DataFrame (df_words)
        combined_df = pd.concat([existing_df, df_words], ignore_index=True)

        # Convert combined DataFrame back to CSV
        csv_data = combined_df.to_csv(index=False)

        # Upload the combined data back to Azure Blob Storage
        blob_client.upload_blob(io.BytesIO(csv_data.encode('utf-8')), overwrite=True, blob_name="df_words.csv")


        print(f"Combined data uploaded to '{blob_name}' in container '{container_name}'.")

    except Exception as e:
        # If blob does not exist, upload the new DataFrame as a new blob
        if 'BlobNotFound' in str(e):
            csv_data = df_words.to_csv(index=False)
            blob_client.upload_blob(io.BytesIO(csv_data.encode('utf-8')), overwrite=True)
            print(f"File '{blob_name}' created and uploaded to container '{container_name}'.")
        else:
            print(f"An error occurred: {e}")





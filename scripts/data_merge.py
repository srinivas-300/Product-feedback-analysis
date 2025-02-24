from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io
import pandas as pd

def merge:

    connection_string = "<YOUR_CONNECTION_STRING>"
    container_name = "<YOUR_CONTAINER_NAME>"
    blob_name = "reviews.csv"
    download_path = "reviews.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)


    with open(download_path,"wb") as file:
        file.write(blobclient.download_blob().readall())


    df1 = pd.read_csv(download_path)

    blob_client = container_client.get_blob_client("new_reviews.csv")

    with open("new_reviews.csv","wb") as file:
        file.write(blobclient.download_blob().readall())

    df2 = pd.read_csv("new_reviews.csv")

    df_merge = pd.concat([df1,df2],ignore_index=True)

    blob_client = container_client.get_blob_client("reviews.csv")

    blob_client.upload_blob(io.BytesIO(df_merge.encode('utf-8')), overwrite=True)

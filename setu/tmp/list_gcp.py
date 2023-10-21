from google.cloud import storage

def list_blobs(bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    # Initialise a client
    storage_client = storage.Client()
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    # List all the blobs in the bucket
    blobs = bucket.list_blobs(prefix=prefix)
    blob_count = 0
    names = []
    # Print the name of each blob
    for blob in blobs:
        # print(blob.name)
        names += [blob.name]
        blob_count += 1

    print(blob_count)

list_blobs("sangraha", "pdfs_text/Assamese/1/")
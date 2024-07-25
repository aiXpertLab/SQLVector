import pandas as pd
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions

def get_collection():
    # Initialize the persistent client
    client = chromadb.PersistentClient(path="./data/chroma", settings=Settings(anonymized_telemetry=False))

    # Get all collection names
    collection_names = client.list_collections()

    # Display collection names
    print("Available collections:")
    for name in collection_names:
        print(f"- {name.name}")

def get_tables_from_chroma():
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))
    collection = client.get_collection("tables_columns")

    results = collection.get()

    table_names = set()
    for metadata in results['metadatas']:
        table_names.add(metadata['table_name'])

    # Print the unique table names
    print("Unique table names:")
    for table in sorted(table_names):
        print(f"- {table}")

def save_csv_to_chroma():

    csv_file = './data/db/TABLES_COLUMNS.CSV'
    df = pd.read_csv(csv_file)

    # Initialize the persistent client
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))

    # Create a new collection or get the existing one
    collection_name = 'tables_columns'

    # Use a simple embedding function
    embedding_function = embedding_functions.DefaultEmbeddingFunction()

    collection = client.create_collection(
        name=collection_name,
        embedding_function=embedding_function
    )

    # Prepare data for insertion
    ids = [str(i) for i in range(len(df))]
    documents = df.apply(lambda row: f"{row['table_name']}, {row['column_name']}, {row['data_type']}", axis=1).tolist()
    metadatas = df.to_dict(orient='records')

    # Insert data into the collection
    collection.add(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )

    print(f"Data from {csv_file} has been successfully embedded and saved to ChromaDB at ./data/chroma3 in the collection '{collection_name}'.")
   
def is_empty():
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))

    # List all collections
    collections = client.list_collections()
    print("Collections:", [c.name for c in collections])

    # If you remember the collection name you used, try to get it
    collection_name = "tables_columns"  # or whatever name you used
    try:
        collection = client.get_collection(collection_name)
        print(f"Collection '{collection_name}' exists")
        
        # Try to get the count of items
        count = collection.count()
        print(f"Number of items in collection: {count}")
        
        # If count > 0, try to get some items
        if count > 0:
            items = collection.get(limit=5)  # Get up to 5 items
            print("Sample items:", items)
        else:
            print("Collection is empty")
    except ValueError:
        print(f"Collection '{collection_name}' does not exist")

def import_csv():
    import pandas as pd
    import chromadb
    from chromadb.config import Settings
    from chromadb.utils import embedding_functions
    import traceback

    # Read the CSV file
    csv_file = 'TABLES_COLUMNS.CSV'
    df = pd.read_csv(csv_file)
    print(f"Read {len(df)} rows from CSV file")

    # Initialize the persistent client
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))

    # Get or create the collection
    collection_name = 'tables_columns'
    embedding_function = embedding_functions.DefaultEmbeddingFunction()

    try:
        collection = client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_function
        )
        print(f"Collection '{collection_name}' accessed successfully")

        # Prepare data for insertion
        ids = [str(i) for i in range(len(df))]
        documents = df.apply(lambda row: f"{row['table_name']}, {row['column_name']}, {row['data_type']}", axis=1).tolist()
        metadatas = df.to_dict(orient='records')

        print(f"Prepared {len(ids)} items for insertion")

        # Insert data into the collection
        print("Starting data insertion...")
        try:
            collection.add(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
            print("Data insertion completed successfully")
        except Exception as insert_error:
            print(f"Error during insertion: {str(insert_error)}")
            print("Traceback:")
            print(traceback.format_exc())

        # Verify the insertion
        count = collection.count()
        print(f"Number of items in collection after insertion: {count}")

        if count > 0:
            sample = collection.get(limit=2)
            print("Sample of inserted data:")
            print(sample)
        else:
            print("Warning: Collection is still empty after insertion attempt")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Traceback:")
        print(traceback.format_exc())

    # Final verification
    final_count = collection.count()
    print(f"Final count of items in collection: {final_count}")

    # Print ChromaDB version
    print(f"ChromaDB version: {chromadb.__version__}")

def mini_chroma():
    import chromadb
    print(f"ChromaDB version: {chromadb.__version__}")

    client = chromadb.PersistentClient(path="./data/chroma3")
    collection = client.get_or_create_collection("test_collection")

    # Try a simple insertion
    try:
        collection.add(
            ids=["1"],
            documents=["This is a test document"],
            metadatas=[{"source": "test"}]
        )
        print("Test insertion successful")
        print(f"Items in collection: {collection.count()}")
    except Exception as e:
        print(f"Error during test insertion: {str(e)}")

    # Try to retrieve the inserted item
    try:
        result = collection.get(ids=["1"])
        print("Retrieved item:", result)
    except Exception as e:
        print(f"Error retrieving item: {str(e)}")
        
def small_csv():
    
    import pandas as pd
    import chromadb
    from chromadb.config import Settings
    from chromadb.utils import embedding_functions

    # Read the CSV file
    csv_file = 'TABLES_COLUMNS.CSV'
    df = pd.read_csv(csv_file)
    print(f"Read {len(df)} rows from CSV file")

    # Initialize the persistent client
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))

    # Get or create the collection
    collection_name = 'tables_columns'
    embedding_function = embedding_functions.DefaultEmbeddingFunction()

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_function
    )
    print(f"Collection '{collection_name}' accessed successfully")

    # Prepare data for insertion
    ids = [str(i) for i in range(len(df))]
    documents = df.apply(lambda row: f"{row['table_name']}, {row['column_name']}, {row['data_type']}", axis=1).tolist()
    metadatas = df.to_dict(orient='records')

    print(f"Prepared {len(ids)} items for insertion")

    # Insert data in batches
    batch_size = 10
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        batch_documents = documents[i:i+batch_size]
        batch_metadatas = metadatas[i:i+batch_size]
        
        try:
            collection.add(
                ids=batch_ids,
                documents=batch_documents,
                metadatas=batch_metadatas
            )
            print(f"Inserted batch {i//batch_size + 1}")
        except Exception as e:
            print(f"Error inserting batch {i//batch_size + 1}: {str(e)}")

    # Verify the insertion
    count = collection.count()
    print(f"Number of items in collection after insertion: {count}")

    if count > 0:
        sample = collection.get(limit=2)
        print("Sample of inserted data:")
        print(sample)
    else:
        print("Warning: Collection is still empty after insertion attempt")

    print(f"ChromaDB version: {chromadb.__version__}")    
    
    # Final verification
    final_count = collection.count()
    print(f"Final count of items in collection: {final_count}")

    if final_count > 0:
        sample = collection.get(limit=5)
        print("\nSample of inserted data (5 items):")
        for i, (id, doc, metadata) in enumerate(zip(sample['ids'], sample['documents'], sample['metadatas']), 1):
            print(f"\nItem {i}:")
            print(f"ID: {id}")
            print(f"Document: {doc}")
            print(f"Metadata: {metadata}")

def medium_csv():

    import pandas as pd
    import chromadb
    from chromadb.config import Settings
    from chromadb.utils import embedding_functions
    import traceback

    # Read the CSV file
    csv_file = 'TABLES_COLUMNS.CSV'
    df = pd.read_csv(csv_file)
    print(f"Read {len(df)} rows from CSV file")

    # Initialize the persistent client
    client = chromadb.PersistentClient(path="./data/chroma3", settings=Settings(anonymized_telemetry=False))

    # Get or create the collection
    collection_name = 'tables_columns'
    embedding_function = embedding_functions.DefaultEmbeddingFunction()

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_function
    )
    print(f"Collection '{collection_name}' accessed successfully")

    # Prepare data for insertion
    ids = [str(i) for i in range(len(df))]
    documents = df.apply(lambda row: f"{row['table_name']}, {row['column_name']}, {row['data_type']}", axis=1).tolist()
    metadatas = df.to_dict(orient='records')

    print(f"Prepared {len(ids)} items for insertion")

    # Insert data one by one
    total_inserted = 0

    try:
        for i in range(len(ids)):
            try:
                if i == 99:  # Extra logging for the 100th item
                    print(f"Attempting to insert 100th item:")
                    print(f"ID: {ids[i]}")
                    print(f"Document: {documents[i]}")
                    print(f"Metadata: {metadatas[i]}")

                collection.add(
                    ids=[ids[i]],
                    documents=[documents[i]],
                    metadatas=[metadatas[i]]
                )
                total_inserted += 1
                print(f"Inserted item {i+1} (Total: {total_inserted}/{len(ids)})")

                if i >= 99:  # Force continuation after 99th item
                    print(f"Forced continuation after item {i+1}")

            except Exception as e:
                print(f"Error inserting item {i+1}: {str(e)}")
                print(traceback.format_exc())
                # Don't break, try to continue
    except KeyboardInterrupt:
        print("\nInsertion process interrupted by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        print(traceback.format_exc())
    finally:
        # Verify the insertion
        count = collection.count()
        print(f"\nNumber of items in collection after insertion: {count}")

        if count > 0:
            sample = collection.get(limit=5)
            print("\nSample of inserted data (up to 5 items):")
            for i, (id, doc, metadata) in enumerate(zip(sample['ids'], sample['documents'], sample['metadatas']), 1):
                print(f"\nItem {i}:")
                print(f"ID: {id}")
                print(f"Document: {doc}")
                print(f"Metadata: {metadata}")
        else:
            print("Warning: Collection is empty after insertion attempt")

        print(f"\nChromaDB version: {chromadb.__version__}")

        # Try to retrieve items after the 99th
        last_items = collection.get(ids=[str(i) for i in range(99, min(104, len(ids)))])
        print("\nAttempting to retrieve items 100-104 (or last few if less):")
        for i, (id, doc, metadata) in enumerate(zip(last_items['ids'], last_items['documents'], last_items['metadatas']), 100):
            print(f"\nItem {i}:")
            print(f"ID: {id}")
            print(f"Document: {doc}")
            print(f"Metadata: {metadata}")

def insert90():
    # Read CSV and prepare data
    df = pd.read_csv('SCHEMA.CSV')
    documents = df.apply(lambda row: f"{row['table_name']}, {row['column_name']}, {row['data_type']}", axis=1).tolist()

    # Initialize ChromaDB and insert data
    client = chromadb.PersistentClient(path="./data/chroma3")
    collection = client.get_or_create_collection("tables_columns")
    collection.add(
        ids=[str(i) for i in range(len(df))],
        documents=documents,
        metadatas=df.to_dict(orient='records')
    )

    print(f"Inserted {collection.count()} items into ChromaDB")

    
if __name__ == "__main__":
    # save_csv_to_chroma()
    get_tables_from_chroma()
    # import_csv()
    # is_empty()
    # mini_chroma()
    # small_csv()
    # medium_csv()
    # insert90()
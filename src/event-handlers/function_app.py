from email.mime import text
from typing import List
import azure.functions as func
import datetime
import json
import logging
import os
from azure.storage.blob import BlobServiceClient
import io
import chromadb
import pdfplumber
#from sentence_transformers import SentenceTransformer
#from langchain.embeddings import SentenceTransformerEmbeddings

app = func.FunctionApp()


def UploadExtractedText(extracted_text: List[str], pmcId: str, title: str) -> None:
    """
    Upload the extracted text to an Azure Blob Storage container.

    Args:
        text (List[str]): The text to upload

    Raises:
        Exception: If blob upload fails
    """
    try:
        # Get connection string from environment variables
        connection_string = os.getenv("StorageConnectionString")
        if not connection_string:
            raise ValueError("StorageConnectionString environment variable not set")

        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get container name from environment variables
        container_name = os.getenv("ExtractedTextContainer")
        if not container_name:
            raise ValueError("ExtractedTextContainer environment variable not set")

        page = 0
        for text in extracted_text:
            page += 1
            logging.info(f"Uploading text for page {page} of document {pmcId}")
            # Create a blob client
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{pmcId}/{page}.txt")

            # Upload the text
            blob_client.upload_blob(text, overwrite=True)

        logging.info("Successfully uploaded extracted text to blob")
    except Exception as e:
        logging.error(f"Failed to upload extracted text: {str(e)}")
        raise

def download_blob_by_name(blob_name: str, container_name: str) -> bytes:
    """
    Download an Azure blob by blob name and return the blob contents as bytes.

    Args:
        blob_name (str): The name of the blob to download
        container_name (str): The name of the container containing the blob

    Returns:
        bytes: The blob contents as bytes

    Raises:
        Exception: If blob download fails
    """
    try:
        # Get connection string from environment variables
        connection_string = os.getenv("StorageConnectionString")
        if not connection_string:
            raise ValueError("StorageConnectionString environment variable not set")

        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download blob contents
        logging.info(f"Downloading blob: {blob_name} from container: {container_name}")
        blob_contents = blob_client.download_blob().readall()

        logging.info(f"Successfully downloaded blob: {blob_name}, size: {len(blob_contents)} bytes")
        return blob_contents

    except Exception as e:
        logging.error(f"Failed to download blob {blob_name}: {str(e)}")
        raise


@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="extract-document-text",
                               connection="ServicebusListener")
@app.service_bus_topic_output(arg_name="output_message", topic_name="text-extracted",
                                connection="ServicebusSender")
def ExtractDocumentTextHandler(azservicebus: func.ServiceBusMessage, output_message: func.Out[str]) -> None:

    message_body = azservicebus.get_body().decode('utf-8')
    logging.info('Received message: %s', message_body)

    parsed_message = json.loads(message_body)

    fileName = parsed_message.get("FileName")
    response_message = {
        "fileName": parsed_message.get("PmcId") + ".txt",
        "title": parsed_message.get("Title"),
        "pmcId": parsed_message.get("PmcId"),
        "doi": parsed_message.get("Doi"),
    }

    logging.info("Processing message: %s", response_message)
    storage_container = os.getenv("StorageContainerName")
    if not storage_container:
        logging.error("StorageContainerName environment variable not set")
        raise ValueError("StorageContainerName environment variable not set")

    logging.info(f"Downloading blob: {fileName} from container: {storage_container}")
    blob_contents = download_blob_by_name(fileName, storage_container)

    processed_text = ProcessPdfText(blob_contents)
    logging.info(f"Processed text length: {processed_text} pages")
    UploadExtractedText(processed_text, response_message["pmcId"], response_message["title"])

    output_message.set(json.dumps(response_message))

def ProcessPdfText(blob_contents: bytes) -> List[str]:
    """
    Process the PDF blob contents and extract text.

    Args:
        blob_contents (bytes): The contents of the PDF blob

    Returns:
        str: Extracted text from the PDF
    """

    try:
        pdf_file_data = io.BytesIO(blob_contents)
        extracted_text = []
        with pdfplumber.open(pdf_file_data) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                extracted_text.append(text)
        return extracted_text
    except Exception as e:
        logging.error(f"Failed to process PDF: {str(e)}")
        raise




@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="process-extracted-text",
                               connection="ServicebusListener")
@app.service_bus_topic_output(arg_name="output_message", topic_name="embeddings-generated",
                                connection="ServicebusSender")
def ExtractedTextHandler(azservicebus: func.ServiceBusMessage,  output_message: func.Out[str]) -> None:

    message_body = azservicebus.get_body().decode('utf-8')
    logging.info('Received message: %s', message_body)

    parsed_message = json.loads(message_body)

    response_message = {
        "fileName": parsed_message.get("fileName"),
        "title": parsed_message.get("title"),
        "pmcId": parsed_message.get("pmcId"),
        "doi": parsed_message.get("doi"),
    }

    logging.info("Processing message: %s", response_message)
    extracted_text_container = os.getenv("ExtractedTextContainer")
    if not extracted_text_container:
        logging.error("ExtracedTextContainer environment variable not set")
        raise ValueError("ExtractedTextContainerName environment variable not set")

    logging.info(f"Downloading blob: {response_message['fileName']} from container: {extracted_text_container}")
    blob_contents = download_blob_by_name(response_message["fileName"], extracted_text_container)

    IndexDocument(blob_contents.decode('utf-8'))
    output_message.set(json.dumps(response_message))


def IndexDocument(document_text: str) -> None:
    """
    Index the document text for further processing or storage.

    Args:
        text (str): The text to index

    Raises:
        NotImplementedError: This function is a placeholder and needs implementation
    """
    logging.info(f"Indexing document with {len(document_text)} characters")
    logging.info("\nLoading and indexing PDF into ChromaDB...")
    #chroma_client = chromadb.Client()
    docs = [line.strip() for line in document_text.split('\n') if len(line.strip()) > 20]
    ids = [f"doc_{i}" for i in range(len(docs))]
    
    # embedding_func = SentenceTransformerEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")

   
    # collection = chroma_client.get_or_create_collection(
    #     name="research_docs",
    #     embedding_function=embedding_func
    # )


    # collection.add(documents=docs, ids=ids)
    logging.info(f"Indexed {len(docs)} research documents.")
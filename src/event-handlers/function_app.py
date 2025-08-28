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
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import OpenAI
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
        "fileNames": [],
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

    response_message["fileNames"] = [f"{response_message['pmcId']}/{i + 1}.txt" for i in range(len(processed_text))]
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
        "fileNames": parsed_message.get("fileNames"),
        "title": parsed_message.get("title"),
        "pmcId": parsed_message.get("pmcId"),
        "doi": parsed_message.get("doi"),
    }

    logging.info("Processing message: %s", response_message)
    extracted_text_container = os.getenv("ExtractedTextContainer")
    if not extracted_text_container:
        logging.error("ExtracedTextContainer environment variable not set")
        raise ValueError("ExtractedTextContainerName environment variable not set")

    for file_name in response_message["fileNames"]:
        logging.info(f"Downloading blob: {file_name} from container: {extracted_text_container}")
        blob_contents = download_blob_by_name(file_name, extracted_text_container)
        IndexDocument(blob_contents.decode('utf-8'), response_message["pmcId"], response_message["title"], file_name, response_message["doi"])

    output_message.set(json.dumps(response_message))


def IndexDocument(document_text: str, pmc_id: str, title: str, blob_name: str, doi: str) -> None:
    """
    Submit the document to Azure Search AI for indexing.

    Args:
        text (str): The text to index

    Raises:
        ProcessingErrors: If the document processing fails
    """
    logging.info(f"Indexing document with {len(document_text)} characters")
    logging.info("\nSending document to Azure Search AI for indexing...")
    documentSummary = SummarizeDocument(pmc_id, title, blob_name, doi)
    upload_document_to_azure_search(document_text, documentSummary, pmc_id, title, blob_name, doi)
    # Need variables to index - set metadata pmcid, title, blob name, doi

    logging.info(f"Indexed {blob_name} research document.")

def SummarizeDocument(pmc_id: str, title: str, blob_name: str, doi: str) -> str:
    """
    Summarize the document using - manually - Look into using Azure OpenAI to generate page summary.

    Args:
        pmc_id (str): The PMC ID of the document
        title (str): The title of the document
        blob_name (str): The blob name of the document
        doi (str): The DOI of the document

    Returns:
        str: The summary of the document

    Raises:
        Exception: If summarization fails
    """
    try:
        # Placeholder for actual summarization logic using Azure OpenAI
        summary = f"""
        Title: {title}
        PMC ID: {pmc_id}
        Blob Name: {blob_name}
        DOI: {doi}
        """
        logging.info(f"Generated summary for document {pmc_id}")
        return summary
    except Exception as e:
        logging.error(f"Failed to summarize document {pmc_id}: {str(e)}")
        raise

def getDocumentPageFromFileName(blob_name: str) -> int:
    """
    Get the page number of the document from its blob name.

    Args:
        blob_name (str): The name of the blob

    Returns:
        int: The page number of the document
    """
    try:
        # Extract page number from blob name (assuming format is like "docname/1.pdf")
        page_number = int(blob_name.split("/")[-1].split(".")[0])
        return page_number
    except Exception as e:
        logging.error(f"Failed to extract page number from blob name {blob_name}: {str(e)}")
        raise

def upload_document_to_azure_search(document_text: str, document_summary: str, pmc_id: str, title: str, blob_name: str, doi: str) -> None:
    """
    Upload the document to Azure Search for indexing.

    Args:
        document_text (str): The text of the document
        document_summary (str): The summary of the document
        pmc_id (str): The PMC ID of the document
        title (str): The title of the document
        blob_name (str): The blob name of the document
        doi (str): The DOI of the document

    Raises:
        Exception: If the upload fails
    """
    try:
        logging.info(f"Uploading document {blob_name} to Azure Search...")

        pageNumber = getDocumentPageFromFileName(blob_name)
        record_to_upload = {}
        record_to_upload["id"] = f"{pmc_id}-{pageNumber}"
        record_to_upload["pmcId"] = pmc_id
        record_to_upload["doi"] = doi
        record_to_upload["title"] = title
        record_to_upload["pageText"] = document_text
        record_to_upload["pageNumber"] = pageNumber
        record_to_upload["fileName"] = blob_name
        record_to_upload["summary"] = document_summary
        record_to_upload["vector"] = getEmbeddedSummary(document_text)

        search_client = SearchClient(
            endpoint=os.getenv("AI_SEARCH_ENDPOINT"), # type: ignore
            index_name=os.getenv("AI_SEARCH_INDEX_NAME"), # type: ignore
            credential=AzureKeyCredential(os.getenv("AI_SEARCH_KEY")), #type: ignore
        )

        search_client.merge_or_upload_documents(
            documents=[record_to_upload]
        )

        
    except Exception as e:
        logging.error(f"Failed to upload document {blob_name} to Azure Search: {str(e)}")
        raise

def getEmbeddedSummary(document_text: str) -> List[float]:
    """
    Get the embedded summary of the document using a sentence transformer model.

    Args:
        document_text (str): The text of the document

    Returns:
        List[float]: The embedded summary of the document

    Raises:
        Exception: If embedding fails
    """
    try:
        openai_client = OpenAI(api_key=os.getenv("OpenAIKey"))

        # Use OpenAI to generate a short summary of the text before generating embeddings
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": f"As an experienced biological researcher. Summarize the following text:\n\n{document_text}"
                }
            ]
        )

        summary = response.choices[0].message.content

        response = openai_client.embeddings.create(
            input=summary,
            model="text-embedding-3-small",
            dimensions=1536

        )
        return response.data[0].embedding
    
    except Exception as e:
        logging.error(f"Failed to get embedded summary: {str(e)}")
        raise
# Use the official Python image as a base
FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Set working directory
WORKDIR /home/site/wwwroot

# Copy function app code
COPY src/event-handlers/ .

# Install dependencies
COPY src/event-handlers/requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Expose port for Azure Functions host
EXPOSE 80

# Start Azure Functions host
CMD ["/azure-functions-host/Microsoft.Azure.WebJobs.Script.WebHost"]

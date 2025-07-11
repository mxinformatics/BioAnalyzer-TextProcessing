## BioAnalzyer Event Handlers for Processing Text

Initial setup

''''
uv venv --python 3.12

uv init

source .venv/bin/activate

func init --python

uv add -r requirements.txt

func new --name ExtractDocumentTextHandler 

````


## Start the function host


````
func host start --port 7077
````


Close Port

````
sudo lsof -i -P -n | grep LISTEN | grep 7077
kill -9 67814
````
# The Project



--

# The Pipeline

### Prerequisites
 
* Python 3.6   
* pip    
* Mongo Database running at `mongodb://localhost:27017/`
* git

### Setup

```
$ git clone https://github.com/appallicious/harvist.git
$ pip install -r requirements.txt
```

Set the parameters in `parameters.json` to your liking. Then run:

```
$ python pipeline.py
$ python chunk_data.py
```

This can take some time depending on how many days you choose to work with and how finely you want to chunk your data.
# SFMTA Vehicle Prediction Capstone

## The Project

This is the capstone project of my 2018 Galvanize Bootcamp. It predicts the travel time of SFMTA vehicles using the time of day, previous vehicle performance, and prior route performance. As of writing, it can predict, on the 33 bus headed south east:

* A 50 minute trip with an error of 4 minutes
* A 25 minute section with an error of 2.5 minutes
* An 8 minute section with an error of 1 minutes

I built a both linear regression and random forest model, both of which were tuned for low-variance with ridge regularization and low tree-depth, respectively. 

The sampling rate of the data would not allow granular predictions between individual stops. I therefore divided the trip into sections based loosely on the Nyquist–Shannon Theorem, and made predictions on these sections.

These models, and their results, can be viewed as jupyter notebooks in the `/notebooks` directory.

Further information can be found in my [one page write-up](https://docs.google.com/document/d/1k_5MV3CBmbHYw2bcVYxNWOdpRTH3LWT4CIO-prtqW9Q/edit?usp=sharing) and in the `/proposal` directory.  

## The Pipeline

The data was, to put it *very* light, dirty, so I built the best pipeline I could to handle it. It is resilient, flexible and portable - below are instruction for you to run it yourself, on whichever bus line/direction you want.

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
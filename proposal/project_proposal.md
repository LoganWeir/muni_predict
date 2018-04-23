Logan Weir    
2018-4-16

# *Less Waitin', Less Hatin'*  

##An arrival time prediction method for the San Francisco MUNI System.

*There once was an happy commuter*   
*With an arrival time on his computer*   
*He was ready to go*    
*But his bus would not show*    
*In the end, he just rented a scooter*

I intend to develop a model to predict the time a San Francisco MUNI bus will arrive at a specified stop. The aim is to out-perform the current predictions suppiled by MUNI via [NextBus](https://www.nextbus.com/#!/sf-muni/E/E____I_F00/4532/4503), and, ideally, the predictions of [Swiftly](http://www.tryswiftly.com/). 

### Why?   

In the last year, MUNI buses only arrived to stops at their scheduled times 57% of the time, [as reported by the SFMTA](https://www.sfmta.com/reports/percentage-time-performance), well below the 85% performance mandated by the city charter. However, even the predictions made using real-time data do not preform that well. According to [a study by Swiftly in late 2015](http://www.tryswiftly.com/blog/2015/12/23/san-francisco-transit-prediction-accuracy-how-swyft-helps-you-commute-smarter-1), NextBus predictions on Muni arrival times are only correct 70% of the time, and are much worse during high-volume periods.

Improving arrival time predictions could have serious economic rammifications. Time wasted, either waiting for a late bus or arriving at a stop early due to an erroreous prediction, can be fiscally quantified. [This report](http://www.isr.umd.edu/NEXTOR/pubs/TDI_Report_Final_10_18_10_V3.pdf) estimated the economic cost of transportation system delays in the U.S. in 2007 at 32.9 billion dollars. Additionally, accurate predictions of arrival times can promote greater usage of public transportation.

### What?

The bulk of my data is in the form of Automatic Vehicle Location (AVL) data, located [here](https://data.sfgov.org/Transportation/Raw-AVL-GPS-data/5fk7-ivit). My interpretation of the data thus far is: each observation is a handful of features, such as speed and location, recorded approximately every minute, for every bus, for that last 4 years. I am fully aware of the assumptions I am making about the data, and will first and foremost need to test these. The very act of estimating the size of this data will be a first step towards approaching it.  

Additionally, the routes that these buses take can change, and I will need to incorporate those changes into my model. Data about historic route changes is [here](http://transitfeeds.com/p/sfmta/60?p=4), in the standard GTFS format. Initially I will attempt to find routes and stops that have not undergone significant changes over the course of the available data. As I expand the scope of the project to other stops and bus lines, it will become necessary to handle route changes in my model, and how to weight data from previous variations of the route in question when training my model. 

Due to the quantity and complexity of the data I'm working with, I intend to begin with a highly constrained subsection of data from which I will work outwards, expanding the scope of my prediction model only when I'm confident in the portability of my data pipeline to other stops and buses. Focusing on a constrained model will allow me to spend time early on building models and not just cleaning data.

Once I have built an ETL pipeline for my data, I can incorporate additional data. There is, in the same FTP repository that contains the AVL data, a dataset of historic predictions made by NextBus. These can be used to validate and compare my own predictions. 

External data to add to my model will need to be available in real-time if I wish to incorporate it into any present predictions. The datasets I will work to include are Traffic and Weather. Weather has live and historic data, whereas Traffic and live data but uncertain or heterogenous historic data. 


### When?

My first job will be to find and gather the subsections of AVL data with which I will work. This data will be loaded into a database (MongoDB), and with it I will build a graph database (Neo4j). The graph database will evolve incrementally, starting with just three nodes connected sequentially by 2 edges, where nodes are stops and edges are the routes that connect them.

For this first graph model I will attempt to predict the travel time along the second edge. The features I will calculate for each bus involve travel time and stop time; all edge and node intervals of the previous bus, and the preceeding edge and node intervals of the current bus. All times will be normalized, and in seconds. 

When predicting across multiple edges, the predicted interval will be the sum of each edge prediction, and edge predictions will be made from preceeding data and predicted intervals as nodes become further from the actual vehicle.

I am operating with the assumption of daily, seasonal fluctuations in my data due of morning and evening rush hours. I will thusly categorize my data into certain intervals:

1. Morning Rush Hour
2. Midday
3. Evening Rush Hour
4. Off-Hours
5. Weekends

My assumption with weekends, which needs to be verified, is that they do not experience the same seasonality as weekdays (rush hour). Additionally, I would like to try and cluster time deltas, with the hopes of discovering quantitatively significant clusters.

Each node will have, for each categorical time interval:    
• Average time of bus at stop    
• Std. Dev. of time of bus at stop

Each edge will have, for each time interval:      
• Average travel time along edge   
• Std. Dev. of time along edge    

When a non-temporal model has been developed, I will diverge into time series analysis model, with the hope of ensembling the two together.



(Metrics for Evaluation)

### How?

(Visualization)



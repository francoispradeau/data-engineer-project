# Movie database Project

## Database design
I used MySQLWorkbench to design the model. Files are all in the top-level dbmodel folder, 
along with a PostScript diagram of the tables and relationships.
Note that this is an Sql (OODB) approach that will work well for medium volumes of data. If the volume is too high,
we might need to plan on a noSql database solution.
## Database csv ingest
### Scope
Given the problem at hand, only the move_database.csv is needed for import at this time. 
Also given the nature of the project (e.g. a homework assignment), this is by no means a 'production-ready' project:
1. The csv dataset is not ideal and issues were found with it that did not fully get solved.
1. The current process is single-threaded and will only scale so much. For larger datasets, a Spark solution would 
maybe be needed as it would scale much better.
1. The output of the files was not tested for loading into the database, but the objects are pretty much one-to-one with
the designed database tables.
1. Unit tests were added but do not cover enough use-cases

Choice was made in the code to import movies even if some metadata associated with it could not be loaded. It would be 
rather easy to change that particular option. With that approach, the rate of success was 99.4% of the lines getting 
ingested

### Technology chosen
I ended up picking Scala as it is my preferred JVM language, although it could have been
done as efficiently in Java. I used IntelliJ CE for the IDE, and checked in the project if need be

## Design task
An initial design will be comprised of:
###Ingest pipeline
Based on the location the files are being dropped of in, we could either depend on a trigger on file drop-off (e.g. S3)
or create a scheduled task (AWS Lambda, others...) that will trigger the ingest every month. 
That trigger would: 
1. Call the ingest code and output the data to a location.
1. Upon completion, call another process to load that data into the database
Note that the system can scale easily if multiple files are dropped. The load on the database
when inserting new objects can be remediated by scheduling it off-peak hours.
The data will be stored in the chosen database (SQL) initially.

###Data serving
The most common and flexible solution is to provide a service with a REST API to access the data.
Based on the requirements, this service (written possibly in Java with dropwizard or SpringBoot, or Python Flask) would
have the following end-points:

####GET /productioncompanies/: 
Get a list of production companies available in the database.
Additional (optional) query parameter: production company name. (URL: /productioncompanies?name="...") to query a given 
company, and if need be, search options
Note that the reply will need to be paged as data will likely be large.
####GET /productioncompanies/{prodcompanyId}/balancesheet?year=YYYY: 
The year query parameter is optional, with using current year as default
Get a json with the following information:
```
{
  "year": 2019,
  "budget": 123456.1,
  "revenue": 234567.2,
  "profit": 34567.3
}
``` 
####GET /productioncompanies/{prodcompanyId}/releases?year=YYYY&genres=[ids]: 
List of releases by year and genres. Note that defaults can be used if 
query parameters are not specified (current year, all movies)
Returned Json:
```$xslt
{
  "year": 2019,
  "genres": [
      {
        "genreId" : 123,
        "genreName" : "genre",
        "movies" : [ 
            { "id": 123}, ... 
        ]
      }
   ]
}  
```
Note that an additional `verbose` query parameter could be used to get full movie information.

Further API would be defined similarly for the other requirements (the deliverables specifically says to not define all routes...)

This microservice can be deployed on an infrastructure within AWS - Kubernetes or Mesos are options, or possibly just an EC2
instance behind a load balancer (AWS ELB & ASG - Auto-scaling group). 
The advantages of using an infrastructure is to be able to put authentication as a separate layer rather than within 
the service itself. Most of the authentication can also reside in AWS (Cognito/IAM).
Auto-scaling is also a concept that can be implemented easily within an infrastructure by monitoring key metrics from 
the service (latency, 400s/500s, memory usage, etc...) 
 

# Erlang-BigData-MapReduce
Final project in Functional Programming in Concurrent and Distributed Systems course.

Video of the project in [Youtube] (https://www.youtube.com/watch?v=vz8TKIhzUdE).

**About the project**

The purpose of this project is to process big data from dblp website (https://dblp.org/) with map-reduce algorithm.

The database is a XML file from dblp website that was converted to CSV file. 

Credit for the conversion code: [dblp-to-csv](https://github.com/ThomHurks/dblp-to-csv)

The master split the data to other 4 computers/terminals, each one of get CSV file and parse it.

Credit for parsing CSV code: [parse-csv](https://gist.github.com/artefactop/7ae92213674810d715d7). 

After finish parsing start process the data (the map part from the map-reduce) and send the answer back to the master. The master reduce the answers and use it the create table and graph with graphviz. 

Credit for creating graph with graphviz code: [erlang-graphviz](https://github.com/glejeune/erlang-graphviz). 

**How to run the program**

On the main computer:

```erl -name master@ip -setcookie dblp```

```erl -name wx@ip -setcookie dblp```

On the other compuetrs/terminals:

```erl -name PCi@ip -setcookie dblp``` Where 'i' is a number between 1 to 4

Make sure to change the ip in PCnames.hrl file!

Compile all the files in all computers:

```c(parse_csv), c(graphviz), c(mapReduce1), c(mapReduce2), c(master), c(local_server), c(makeTable), c(wxGui)```

Finally in the master computer write: ```wxGui:start_link()``` and the application will run

Now all you need to do is to choose an author name and click "search".



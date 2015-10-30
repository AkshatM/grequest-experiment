# About

This is an experiment in implementing asynchronous network requests with [grequests](https://github.com/kennethreitz/grequests), done for fun and to see just how much speedup could be obtained by abandoning serial network connections. 

It fetches all movies currently in theaters from the RottenTomatoes API, proceeds to grab the relevant IMDB page, parses it for image counts and returns them. An estimated 200 or so connections need to be made.

The speedups were impressive: a serial version of this program took about two to five minutes to complete; the asynchronous version took 4-5 seconds. 

Grequests is a beautiful tool.
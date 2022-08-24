from a2_stencil import DbStreamer;

import inspect;

if __name__ == "__main__":

    ## TODO: Initialize the DbStreamer object
    db_streamer = None
    db_streamer = DbStreamer(host = "localhost", user = "root", password = "Vjs@1234569a", database = "moviesdatabase")
    # Do not modify any code below this line. Once you fill in the code for each of the methods, the output will be logged in separate files in the query_ouputs directory via this script.
    attrs = (getattr(db_streamer, name) for name in dir(db_streamer))
    methods = filter(inspect.ismethod, attrs)
    for q_method in methods:
        if q_method.__name__.startswith('q'):
            print(q_method.__name__)
            with open('query_outputs/{}.txt'.format(q_method.__name__), 'w') as f:
                data_row_list = DbStreamer.get_rows(q_method())
                for row in data_row_list:
                    f.write(str(row))
                    f.write("\n")

import MySQLdb

class DbStreamer:

    @staticmethod
    def get_rows(data):
        data_rows = []
        for row in data:
            data_rows.append(row)
        return data_rows


    def __init__(self, host, user, password, database):
        self.conn = MySQLdb.Connection(host=host,
                                       user=user,
                                       passwd=password,
                                       db=database,
                                       charset="utf8",
                                       use_unicode=True)
        _cursor = self.conn.cursor()
        return

    def get_connection(self):
        return self.conn

    def close_connection(self):
        self.conn.commit()
        self.conn.close()
        return
    
    def get_tables(self):
        sql = "SHOW TABLES;"
        
        _cursor = self.conn.cursor()
        _cursor.execute(sql)
        data = _cursor.fetchall()

        return data

    def q0(self):
        sql = "SELECT DATE('2020-01-23');"
        _cursor = self.conn.cursor()
        _cursor.execute(sql)
        data = _cursor.fetchall()
        return data

    # TODO: Add your logic for each of the questions in the corresponding methods provided below. Each method should return a list of tuples/rows without the header.
    def q1(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT ID FROM MOVIE_DETAILS ORDER BY VOTE_COUNT DESC LIMIT 5;"
        _cursor.execute(sql)
        data = _cursor.fetchall()

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q2(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT ID FROM MOVIE_DETAILS WHERE BUDGET = (SELECT MAX(BUDGET) FROM MOVIE_DETAILS);"
        _cursor.execute(sql)
        data = _cursor.fetchall()

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q3(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title, runtime from (select m.id, m.title, m.runtime from movie_details m where m.runtime = (select MAX(runtime) from movie_details) or m.runtime = (select MIN(runtime) from movie_details)) as FILTERED order by runtime desc;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q4(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT id, release_date from (select m.id, m.release_date from movie_details m where m.release_date = (select MAX(release_date) from movie_details) or m.release_date = (select MIN(release_date) from movie_details where release_date > '0000-00-00')) as FILTERED order by release_date desc;"
        _cursor.execute(sql)
        data = _cursor.fetchall()

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q5(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT title, budget from movie_details where popularity = (select MAX(popularity) from movie_details);"
        _cursor.execute(sql)
        data = _cursor.fetchall()

        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q6(self):
        _cursor = self.conn.cursor()
        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT company_id from company_details where movie_id = (select id from movie_details where popularity = (select MAX(popularity) from movie_details));"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q7(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "WITH OVERFILTERED AS (WITH FILTERED AS (SELECT id FROM movie_details JOIN company_details ON company_details.movie_id = movie_details.id) SELECT id FROM filtered GROUP BY id having count(*) > 2) SELECT count(*) FROM OVERFILTERED;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q8(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "SELECT id from movie_details where revenue - budget = (SELECT MAX(revenue-budget) from movie_details);"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q9(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = " with THIRDFILTER as (with SECONDFILTER as (with FIRSTFILTER as (select g.title as name, g.genre as gen, g.movie_id as movie_id from genres as g join movie_details on movie_details.id = g.movie_id) select name, gen, movie_id, count(movie_id) as cnt from FIRSTFILTER group by movie_id) select name from SECONDFILTER where cnt = (select MAX(cnt) from SECONDFILTER)) select title, genre from genres join THIRDFILTER on THIRDFILTER.name = genres.title;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q10(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "with FIRSTFILTER as (select company_id, company, count(movie_id) as cnt from company_details join movie_details where movie_details.id = company_details.movie_id group by company_details.company_id) select company_id from FIRSTFILTER where cnt = (select MAX(cnt) from FIRSTFILTER);"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q11(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "with FILTER as (select id from movie_details order by vote_count desc limit 5) select company from company_details join FILTER on FILTER.id = company_details.movie_id;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q12(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "with FILTER as (select company_id, company, movie_id, vote_average from company_details as c join movie_details as m on m.id = c.movie_id) select company_id from FILTER group by company_id having min(vote_average) >= 7;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q13(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "with REQUIRED_DETAILS as (with FIRSTFILTER as (select m.id as id, m.title as name, m.popularity as p, k.keyword from movie_details as m join keywords_details as k on m.id = k.movie_id where k.keyword like '%novel%') select id,name,p from (select id,name,p from FIRSTFILTER order by p desc limit 5) as table1 union all (select id,name,p from FIRSTFILTER order by p asc limit 5)) select id from REQUIRED_DETAILS;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q14(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "select genre, avg(vote_count) as avg_vote_count from movie_details join genres on movie_details.id=genres.movie_id group by genre;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data

    def q15(self):
        _cursor = self.conn.cursor()

        # TODO: Add logic here
        # ------------------------------------------------------------------------------------
        sql = "with FILTER as (select m.title, id, genre from movie_details as m join genres as g on m.id=g.movie_id), FILTER1 as (select id, budget from movie_details order by budget desc limit 6) select title, genre from FILTER1 join FILTER on FILTER1.id = FILTER.id order by budget desc;"
        _cursor.execute(sql)
        data = _cursor.fetchall()


        # ------------------------------------------------------------------------------------
        # Do not edit below this line, otherwise the autograder won't be able to evaluate your code.

        return data


if __name__ == "__main__":
    ## ToDO: Init the DbStreamer object
    db_streamer = None
    db_streamer = DbStreamer(host = "localhost", user = "root", password = "Vjs@1234569a", database = "moviesdatabase")
    # print(db_streamer.q0())


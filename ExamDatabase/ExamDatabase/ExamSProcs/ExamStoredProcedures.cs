using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

public partial class StoredProcedures
{
    [SqlProcedure]
    public static SqlInt32 ListOfMoviesBackOnCertainDate (SqlString date)
    {
        SqlConnection conn = new SqlConnection();
        conn.ConnectionString = "Context Connection=true";
        SqlCommand cmd = new SqlCommand();
        cmd.Connection = conn;

        int temp;
        bool isNaN = Int32.TryParse(date.ToString(), out temp);

        if (date.ToString() == "" || date == null || isNaN)
        {
            return 0;
        }

        SqlParameter dateParam = new SqlParameter();
        dateParam.Value = date;
        dateParam.Direction = ParameterDirection.Input;
        dateParam.SqlDbType = SqlDbType.NVarChar;
        dateParam.ParameterName = "@date";
        cmd.Parameters.Add(dateParam);

        cmd.CommandText = "SELECT mo.title, " +
                          "mc.movieType, " +
                          "m.firstName, " +
                          "m.lastName, " +
                          "r.rentalDate, " +
                          "r.returnDate " +
                          "FROM Member AS m " +
                          "JOIN Rental AS r ON m.memberID = r.memberID " +
                          "JOIN Movie AS mo ON r.rentalID = mo.rentalID " +
                          "JOIN Category AS c ON mo.categoryID = c.categoryID " +	   
	                      "JOIN copyMovieJoined AS cmj ON mo.movieID = cmj.movieID " +
                          "JOIN MovieCopy AS mc ON cmj.movieCopyID = mc.MovieCopyID " +
                          "WHERE r.returnDate = @date;";
        
        conn.Open();
        SqlContext.Pipe.ExecuteAndSend(cmd);
        conn.Close();
        conn.Dispose();
        cmd.Dispose();

        return 1;
    }

    [SqlProcedure]
    public static SqlInt32 MembersMovieListToReturn(SqlString name)
    {
        SqlConnection conn = new SqlConnection();
        conn.ConnectionString = "Context Connection=true";
        SqlCommand cmd = new SqlCommand();
        cmd.Connection = conn;

        int temp;
        bool isNaN = Int32.TryParse(name.ToString(), out temp);

        if (name.ToString() == "" || name == null || isNaN)
        {
            return 0;
        }

        SqlParameter nameParam = new SqlParameter();
        nameParam.Value = name;
        nameParam.Direction = ParameterDirection.Input;
        nameParam.SqlDbType = SqlDbType.NVarChar;
        nameParam.ParameterName = "@name";
        cmd.Parameters.Add(nameParam);

        cmd.CommandText = "SELECT m.firstName, " +
                          "m.lastName, " +
                          "r.rentalDate, " + 
                          "r.returnDate, " +
                          "SUM((DATEDIFF(day, r.rentalDate, r.returnDate) * r.costPerDay)) AS totalCost, " +
                          "COUNT(*) AS NrOfMovies " +
	                      "FROM Member AS m " +
                          "JOIN Rental AS r ON m.memberID = r.memberID " +
                          "JOIN Movie AS mo ON r.rentalID = mo.rentalID " +
                          "WHERE m.firstName = @name " +
                          "GROUP BY m.firstName, " +
                          "m.lastName, " +
                          "r.rentalDate, " + 
                          "r.returnDate";	   

        conn.Open();
        SqlContext.Pipe.ExecuteAndSend(cmd);
        conn.Close();
        conn.Dispose();
        cmd.Dispose();
        return 1;
    }

    [SqlProcedure]
    public static SqlInt32 NrOfMoviesInCertainCategory(SqlString category)
    {
        SqlConnection conn = new SqlConnection();
        conn.ConnectionString = "Context Connection=true";
        SqlCommand cmd = new SqlCommand();
        cmd.Connection = conn;

        int temp;
        bool isNaN = Int32.TryParse(category.ToString(), out temp);

        if (category.ToString() == "" || category == null || isNaN)
        {
            return 0;
        }

        SqlParameter categoryParam = new SqlParameter();
        categoryParam.Value = category;
        categoryParam.Direction = ParameterDirection.Input;
        categoryParam.SqlDbType = SqlDbType.NVarChar;
        categoryParam.ParameterName = "@category";
        cmd.Parameters.Add(categoryParam);

        cmd.CommandText = "SELECT COUNT(*) AS NrOfMoviesInCategory, " +
                          "c.movieCategory " +
                          "FROM Movie AS mo " +
                          "JOIN Category AS c ON mo.categoryID = c.categoryID " +
                          "WHERE c.movieCategory = @category " +
                          "GROUP BY c.movieCategory";

        conn.Open();
        SqlContext.Pipe.ExecuteAndSend(cmd);
        conn.Close();
        conn.Dispose();
        cmd.Dispose();
        return 1;
    }
}

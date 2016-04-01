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

        if (date.ToString() == "" || date == null || isNaN || temp < 0 || temp > 10)
        {
            return 0;
        }

        SqlParameter dateParam = new SqlParameter();
        dateParam.Value = date;
        dateParam.Direction = ParameterDirection.Input;
        dateParam.SqlDbType = SqlDbType.Date;
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
    public static void MembersMovieListToReturn()
    {
                
    }

    [SqlProcedure]
    public static void NrOfMoviesInCertainCategory()
    {

    }
}

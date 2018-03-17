using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;


namespace sqlBulkCopy
{
    public class BulkCopy_Azure_to_dbc02
    {
        string dstConnStr = ConfigurationManager.ConnectionStrings["azure"].ConnectionString;
        string srcConnStr = ConfigurationManager.ConnectionStrings["source"].ConnectionString;


        string sql_full = "select InvoiceID,Booking_Owned from trReservations";
        //"select * from _factBookings";

        string dstTable = "tmp"; //"_factBookings";


        public void PerformBulkCopyDifferentSchema()
        {


            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                //----


                //----

                SqlCommand myCommand =
                    new SqlCommand(sql_full, srcConn);



                SqlDataReader reader = myCommand.ExecuteReader();
                // open the destination data
                using (SqlConnection dstConn =
                            new SqlConnection(dstConnStr))
                {
                    // open the connection
                    dstConn.Open();
                    using (SqlBulkCopy bulkCopy =
                        new SqlBulkCopy(dstConn.ConnectionString))
                    {
                        bulkCopy.BatchSize = 500;
                        bulkCopy.NotifyAfter = 1000;
                        bulkCopy.SqlRowsCopied +=
                            new SqlRowsCopiedEventHandler(bulkCopy_SqlRowsCopied);
                        bulkCopy.BulkCopyTimeout = 12000; // 200 minutes timeout !!!!!

                        bulkCopy.DestinationTableName = dstTable;
                        bulkCopy.WriteToServer(reader);
                    }
                }
                reader.Close();


            }
        }



        void bulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            System.Console.WriteLine("rows copied:" + e.RowsCopied);
        }

    }


}
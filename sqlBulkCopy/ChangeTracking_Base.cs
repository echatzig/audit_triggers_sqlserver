#define ACTION_ADD_ROWS

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

    abstract class ChangeTracking_Base
    {
        string srcConnStr = ConfigurationManager.ConnectionStrings["source"].ConnectionString;
        string dstConnStr = ConfigurationManager.ConnectionStrings["destin"].ConnectionString;



        public abstract string sql_full { get; }

        public abstract string srcTable { get; }
        public abstract string dstTable { get; }

        public abstract string sql_incr { get; }

        public abstract int[] PKColOrdinals { get; }




        /*
            --- enable

            alter database Unique_Golf set change_tracking=ON (change_retention=14 days, auto_cleanup = on)

            alter table trReservations enable change_tracking with (track_columns_updated=off)

            grant view change tracking on trReservations to transfer

            --- disable
            alter table trReservations disable change_tracking 

            alter database Unique_Golf set change_tracking=OFF

        */
        public void PerformBulkCopyDifferentSchema()
        {
            Int64 vStartVersionID;


            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                //----

                var fatUglyTransaction = srcConn.BeginTransaction(IsolationLevel.Serializable);

                //---- lock the entire TABLE !!!
                //      --------------------
                //         --------------

                string sql_2 = "select top 1 * from " + srcTable + " with(tablockx)";
                SqlCommand sqlCmd2 = new SqlCommand(sql_2, srcConn, fatUglyTransaction);
                sqlCmd2.ExecuteNonQuery();

                // note that IDENT_CURRENT returns the latest row
                //      we need to start the incremental updates INCLUDING that row
                //           so we will use a >= operator

                //string sql3 = @"select StartVersionID = cast(IDENT_CURRENT('" + srcTable + "_audit') as bigint)";
                //string sql3 = @"SELECT top 1 StartVersionID  = cast(case when count(*) = 0 then 0 else ident_current('" + srcTable + "_audit') end as bigint) from " + srcTable + "_audit";


                // when the bulk insert is completed, the incremental should SKIP all existing rows in the _audit, therefore we need to ADD the IDENT_INCR
                string sql3 = @"SELECT EndVersionID = cast( isnull(max(aud_id),0) + IDENT_INCR('" + srcTable + "_audit') as bigint) from " + srcTable + "_audit";

                SqlCommand sqlCmd3 = new SqlCommand(sql3, srcConn, fatUglyTransaction);
                vStartVersionID = (Int64)sqlCmd3.ExecuteScalar();

                //----

                SqlCommand cmdGetCT1 =
                    new SqlCommand(@"
                        insert into Change_Tracking_Version (Table_Name, Change_Tracking_Version)
                        select @srcTable, @StartVersionID
                        ", srcConn, fatUglyTransaction);
                cmdGetCT1.Parameters.AddWithValue("@StartVersionID", vStartVersionID);
                cmdGetCT1.Parameters.AddWithValue("@srcTable", srcTable);

                cmdGetCT1.ExecuteNonQuery();

                //----

                SqlCommand myCommand =
                    new SqlCommand(sql_full, srcConn, fatUglyTransaction);
                myCommand.CommandTimeout = 12000;   // 200 minutes timeout !!!!!



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

                        //bulkCopy.ColumnMappings.Add("ProductID", "ProductID");
                        //bulkCopy.ColumnMappings.Add("ProductName", "Name");
                        //bulkCopy.ColumnMappings.Add("QuantityPerUnit", "Quantity");
                        bulkCopy.DestinationTableName = dstTable;
                        bulkCopy.WriteToServer(reader);
                    }
                }
                reader.Close();

                // ----
                // when the bulk insert is completed, the incremental should SKIP all existing rows in the _audit, therefore we need to ADD the IDENT_INCR
                string sqlCT_2 = @"SELECT EndVersionID = cast( isnull(max(aud_id),0) + IDENT_INCR('" + srcTable + "_audit') as bigint) from " + srcTable + "_audit";


                SqlCommand cmdCT_2 = new SqlCommand(sqlCT_2, srcConn, fatUglyTransaction);
                Int64 vStartVersionID_2 = (Int64)cmdCT_2.ExecuteScalar();

                Debug.Assert(vStartVersionID == vStartVersionID_2);

                // ----
                fatUglyTransaction.Commit();

            }
        }




        public void PerformIncremental()
        {
            Int64 vStartVersionID;
            Int64 vEndVersionID;

            // DataTable sourceData = new DataTable();
            // get the source data
            using (SqlConnection srcConn =
                            new SqlConnection(srcConnStr))
            {
                srcConn.Open();

                string sql2 = @"SELECT Change_Tracking_Version
                    FROM Change_Tracking_Version
                    WHERE Table_Name = @srcTable";
                SqlCommand sqlCmd2 = new SqlCommand(sql2, srcConn);
                sqlCmd2.Parameters.AddWithValue("@srcTable", srcTable);

                vStartVersionID = (Int64)sqlCmd2.ExecuteScalar();

                // select the NEXT item after the current identity - the inc_sql will use >= start and < end
                //string sql3 = @"SELECT top 1 EndVersionID = cast(case when count(*) = 0 then 0 else ident_current('" + srcTable + "_audit') end + IDENT_INCR('" + srcTable + "_audit') as bigint) from " + srcTable + "_audit";
                string sql3 = @"SELECT EndVersionID = cast( isnull(max(aud_id),0) + IDENT_INCR('" + srcTable + "_audit') as bigint) from " + srcTable + "_audit";

                // we will store that value, so that the next iteration will skip the processed rows and start from the next identity (if more rows are added meanwhile)

                SqlCommand sqlCmd3 = new SqlCommand(sql3, srcConn);
                vEndVersionID = (Int64)sqlCmd3.ExecuteScalar();


                SqlCommand myCommand =
                    new SqlCommand(sql_incr, srcConn);
                myCommand.Parameters.AddWithValue("@StartVersionID", vStartVersionID);
                myCommand.Parameters.AddWithValue("@EndVersionID", vEndVersionID);

                SqlDataReader reader = myCommand.ExecuteReader();
                DataTable dt = new DataTable();
                for (var c = 0; c < reader.FieldCount; c++)
                {
                    dt.Columns.Add(reader.GetName(c), reader.GetFieldType(c));
                }

                // index of last field (set to OPERATION)
                int fldOPER = reader.FieldCount - 1;

                object[] obj = new object[reader.FieldCount];

#if ACTION_ADD_ROWS
                // add rows

                while (reader.Read())
                {
                    reader.GetValues(obj);

                    //if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    //    continue;
                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    ////     continue;

                    var dr = dt.Rows.Add(obj);

                    //var idx = dt.Rows.Count;
                    //dr = dt.Rows[idx - 1];

                    Console.WriteLine(reader.GetSqlChars(fldOPER).Value[0]);

                    if (reader.GetSqlChars(fldOPER).Value[0] == 'I')
                    {
                        /* no-op */
                    }
                    if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    {
                        dr.AcceptChanges();
                        dr.Delete();
                    }
                    if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    {
                        dr.AcceptChanges();
                        dr.SetModified();
                    }
                }

#else
                // remove rows
                while (reader.Read())
                {
                    reader.GetValues(obj);

                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'D')
                    ////     continue;
                    //// if (reader.GetSqlChars(fldOPER).Value[0] == 'U')
                    ////     continue;

                    var dr = dt.Rows.Add(obj);

                    // delete I,U rows 
                    dr.AcceptChanges();
                    dr.Delete();
                }
#endif


                SqlConnection dstConn =
                            new SqlConnection(dstConnStr);
                dstConn.Open();

                ///
                var fatUglyTransaction = dstConn.BeginTransaction(IsolationLevel.Serializable);
                ///


                var sqlCmd = new SqlCommand("select top 0 * from " + dstTable, dstConn, fatUglyTransaction);
                sqlCmd.Parameters.AddWithValue("@dstTable", dstTable);
                SqlDataAdapter da = new SqlDataAdapter(sqlCmd);
                var sqlCB = new SqlCommandBuilder(da);
                sqlCB.ConflictOption = ConflictOption.OverwriteChanges; // generate UPD/DEL commands using PK only in the WHERE clause

                //SqlCommand delCmd = new SqlCommand(@"
                //    delete from _factBookings 
                //    where 
                //            BookingID       = @BookingID
                //    and     Amendments      = @Amendments   
                //    and     BookingStatus   = @BookingStatus
                //    and     Cancellations   = @Cancellations
                //", dstConn, fatUglyTransaction);
                //
                //delCmd.Parameters.Add(new SqlParameter("@BookingID", SqlDbType.VarChar));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@BookingID"].SourceColumn = "BookingID";
                //
                //delCmd.Parameters.Add(new SqlParameter("@Amendments", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@Amendments"].SourceColumn = "Amendments";
                //
                //delCmd.Parameters.Add(new SqlParameter("@BookingStatus", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@BookingStatus"].SourceColumn = "BookingStatus";
                //
                //delCmd.Parameters.Add(new SqlParameter("@Cancellations", SqlDbType.TinyInt));
                //delCmd.Parameters["@BookingID"].SourceVersion = DataRowVersion.Original;
                //delCmd.Parameters["@Cancellations"].SourceColumn = "Cancellations";
                //
                //da.DeleteCommand = delCmd;

                da.DeleteCommand = sqlCB.GetDeleteCommand();
                da.UpdateCommand = sqlCB.GetUpdateCommand();
                da.InsertCommand = sqlCB.GetInsertCommand();

                da.DeleteCommand.Transaction = fatUglyTransaction;
                da.UpdateCommand.Transaction = fatUglyTransaction;
                da.InsertCommand.Transaction = fatUglyTransaction;

                Console.WriteLine(da.DeleteCommand.CommandText);
                Console.WriteLine(da.UpdateCommand.CommandText);
                Console.WriteLine(da.InsertCommand.CommandText);

                //var delRow = dt.Select(null, null, DataViewRowState.Deleted).FirstOrDefault();
                //DataRow[] delRows = new DataRow[1];
                //delRows[0] = delRow;
                //da.Update(delRows);

                //var delRows = dt.Select(null, null, DataViewRowState.Deleted);


                int counter = 0;
                DataTable dtCp = dt.Clone();
                dtCp.Clear();



                // Stream fs = new FileStream(  "log.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                // StreamWriter sw = new StreamWriter(fs);
                // 

                foreach (DataRow dr in dt.Rows)
                {
                    DataRow[] delRWs = new DataRow[1];
                    dtCp.ImportRow(dr);
                    DataRow drr = dtCp.Rows[0];

                    delRWs[0] = drr;
                    counter++;
                    if (counter % 5000 == 0)
                        Console.WriteLine("{0} counter={1} total={2} %={3}", DateTime.Now,counter, dt.Rows.Count, Math.Round( (double)(counter * 100) / dt.Rows.Count));

                    try
                    {
                        da.Update(delRWs);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("count: {0}", counter);
                        Console.WriteLine(drr.RowState);
                        if (drr.RowState == DataRowState.Deleted)
                        {
                            foreach(var i in PKColOrdinals)
                                Console.WriteLine(drr[i, DataRowVersion.Original]);
                        }
                        else
                        {
                            foreach (var i in PKColOrdinals)
                                Console.WriteLine(drr[i]);
                        }
                        //foreach (var c in dt.PrimaryKey)
                        //{
                        //    if (drr.RowState == DataRowState.Deleted)
                        //    {
                        //        Console.WriteLine(drr[c.Ordinal, DataRowVersion.Original]);
                        //    }
                        //    else
                        //    {
                        //        Console.WriteLine(drr[c.Ordinal]);
                        //    }
                        //}


                        Console.WriteLine(ex.Message);
                        throw;
                    }
                    dtCp.Clear();

                }

                //da.Update(dt);

                fatUglyTransaction.Commit();

                reader.Close();

                dstConn.Close();

                // ----

                SqlCommand cmdSet_CT =
                    new SqlCommand(@"
                        update Change_Tracking_Version
                        set Change_Tracking_Version = @EndVersionID
                        where Table_Name= @srcTable
                        ", srcConn);
                cmdSet_CT.Parameters.AddWithValue("@EndVersionID", vEndVersionID);
                cmdSet_CT.Parameters.AddWithValue("@srcTable", srcTable);
                cmdSet_CT.ExecuteNonQuery();

            }
        }

        private static SqlCommand execNonQuery(SqlConnection srcConn, string sql_1)
        {
            SqlCommand sqlCmd3 = new SqlCommand(sql_1, srcConn);
            sqlCmd3.ExecuteNonQuery();
            return sqlCmd3;
        }

        void bulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            System.Console.WriteLine("rows copied:" + e.RowsCopied);
        }


        public void testPKColumns()
        {

            SqlConnection dstConn =
                        new SqlConnection(dstConnStr);
            dstConn.Open();

            ///
            ///
            var fatUglyTransaction = dstConn.BeginTransaction(IsolationLevel.Serializable);
            ///
            var sqlCmd = new SqlCommand("select top 0 * from " + dstTable, dstConn, fatUglyTransaction); 

            SqlDataAdapter da = new SqlDataAdapter(sqlCmd);

            DataTable dt = new DataTable();
            da.Fill(dt);

            foreach (var c in dt.PrimaryKey)
            {
                Console.WriteLine(c.Ordinal);
                //if (drr.RowState == DataRowState.Deleted)
                //{
                //    Console.WriteLine(drr[c.Ordinal, DataRowVersion.Original]);
                //}
                //else
                //{
                //    Console.WriteLine(drr[c.Ordinal]);
                //}
            }



            // Stream fs = new FileStream(  "log.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            // StreamWriter sw = new StreamWriter(fs);
            // 
            

        }

    }


}

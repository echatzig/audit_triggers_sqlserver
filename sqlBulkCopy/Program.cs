#define ACTION_ADD_ROWS

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


/*
 * https://logicalread.com/sql-server-change-tracking-bulletproof-etl-p1-mb01/#.WX8U_oiGOCg
 */
namespace sqlBulkCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            Mutex mutex = new Mutex(false, "echatzig-sqlBulkCopy");

            // Wait 5 seconds if contended – in case another instance
            // of the program is in the process of shutting down.
            if (!mutex.WaitOne(0, false))
            {
                Console.WriteLine("Another instance of the app is running. Bye!");
                //Console.ReadLine();
                return;
            }

            try
            {
                executeOnce();
            }
            finally
            {
                mutex.ReleaseMutex();
            }
        }


        static void executeOnce()
        {
            bool init = true;
            {
                var b9 = new Users_CT();
                if (init) b9.PerformBulkCopyDifferentSchema();
                else b9.PerformIncremental();
            } 

        }
    }


 
}

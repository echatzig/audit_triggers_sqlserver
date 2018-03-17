using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlBulkCopy
{

    class Users_CT : ChangeTracking_Base
    {

        public override string sql_full { get { return @"
                        SELECT  

	                        [UserID]			,
	                        [Flag]              ,
	                        [UGroupID]          ,
	                        [LName]             ,
	                        [FName]             ,
	                        [email]             ,
	                        [Telephone]         ,
	                        [Extension]         ,
	                        [Mobile]            ,
	                        [Email2]            ,
	                        [Status]            ,
	                        [IT]                ,
	                        [ITResource]        ,
	                        [ApproveTickets]    ,
	                        [TicketsApprovedBy] ,
	                        [editAgents]        


                        from Users res

            "; } }

        public override string srcTable { get { return @"Users"; } }
        public override string dstTable { get { return "_dimUsers"; } }

        public override string sql_incr { get { return @"
SELECT  

	                        [UserID]			,       -- PK:0
	                        [Flag]              ,
	                        [UGroupID]          ,       -- PK:2
	                        [LName]             ,
	                        [FName]             ,
	                        [email]             ,
	                        [Telephone]         ,
	                        [Extension]         ,
	                        [Mobile]            ,
	                        [Email2]            ,
	                        [Status]            ,
	                        [IT]                ,
	                        [ITResource]        ,
	                        [ApproveTickets]    ,
	                        [TicketsApprovedBy] ,
	                        [editAgents]        ,


		Aud_Op [Operation]

FROM Users_audit res
WHERE res.aud_id >=  @StartVersionID 
and   res.aud_id < @EndVersionID
order by res.aud_id

        "; } }

        public override int[] PKColOrdinals { get { return new int[] { 0, 2 }; } }


    }




}

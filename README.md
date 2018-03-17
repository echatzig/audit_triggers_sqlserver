# ChangeTracking_SQLServer

I spend a lot of time trying to implement a solution that updates a Data Warehouse from a source OLTP database using SSIS.

In the end, I gave up - for me SSIS is a productivity killer. It does not support any type of reuse, and the point & click
approach in development seems to be suitable for novice users.

So I decided to implement Change Tracking in C# - this will allow for extensibility and reuse.

Although the code requires some cleanup, it still worth being published. The main idea which I inspired from
Tim Michel https://www.timmitchell.net/post/2016/01/22/using-change-tracking-in-ssis/ is to use a helper table
namely [etl].[Change_Tracking_Version] that holds the last change tracking version that has been synched to 
the destination server.

I have thus impmeneted two methods:

1. PerformBulkCopyDifferentSchema which does the initial copy between the source and the destination server
2. PerformIncremental which uses change tracking to detect and transfer updates only.


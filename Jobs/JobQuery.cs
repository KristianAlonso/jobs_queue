using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobsQueue.Jobs
{
    internal static class JobQuery
    {
        public static string GetJobs (string? where = null, string? orderBy = null) => @"
            SELECT
                jobs_tenant.*
            FROM jobs_tenant
            " + (where is null ? "" : @"WHERE
            " + where) + @"
            " + (orderBy is null ? "" : @"ORDER BY
            " + orderBy) + @"
            ;";

        public static string UpdateStatus(uint[] ids) => @"
            UPDATE jobs_tenant SET
                jobs_tenant.status = @status,
                jobs_tenant.message = IF(@message = '', NULL, @message)
            WHERE
                jobs_tenant.id " + (ids.Length == 1
                    ? $"= '{ids[0]}'"
                    : $"IN ({string.Join(",", ids.Select(id => id.ToString()))})"
                ) + @"
            ;";
    }
}

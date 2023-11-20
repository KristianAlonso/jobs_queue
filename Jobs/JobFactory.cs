using System;
using System.Data;

namespace JobsQueue.Jobs
{
    public static class JobFactory
    {
        public static Job Build(IDataRecord dataRecord)
        {
            return new Job
            {
                Id = (uint) dataRecord.GetInt32(dataRecord.GetOrdinal("id")),
                CompanyKey = (uint) dataRecord.GetInt32(dataRecord.GetOrdinal("company_key")),
                EntityId = dataRecord.GetGuid(dataRecord.GetOrdinal("entity_id")),
                Kind = Enum.Parse<JobKind>(dataRecord.GetString(dataRecord.GetOrdinal("kind"))),
                Priority = Enum.Parse<JobPriority>(dataRecord.GetString(dataRecord.GetOrdinal("priority"))),
                Status = Enum.Parse<JobStatus>(dataRecord.GetString(dataRecord.GetOrdinal("status"))),
                ExecuteAt = dataRecord.GetDateTime(dataRecord.GetOrdinal("execute_at")),
                Message = dataRecord.GetValue(dataRecord.GetOrdinal("message")) as string,
                CreatedAt = dataRecord.GetDateTime(dataRecord.GetOrdinal("created_at")),
                UpdatedAt = dataRecord.GetValue(dataRecord.GetOrdinal("updated_at")) as DateTime?,
            };
        }
    }
}

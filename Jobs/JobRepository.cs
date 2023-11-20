using System.Data;
using System.Text;

namespace JobsQueue.Jobs
{

    public sealed class JobRepository
    {
        private readonly string? ConnectionString;

        public JobRepository(IConfiguration configuration)
        {
            ConnectionString = configuration.GetValue<string>("ConnectionStrings:MySQLHub");
        }

        public async Task<List<Job>> JobsGetAsync()
        {
            using var command = new MySqlConnector.MySqlCommand();
            command.Connection = new MySqlConnector.MySqlConnection(ConnectionString);
            command.CommandType = CommandType.Text;

            var where = new StringBuilder();

            where.Append($"jobs_tenant.status IN ('{JobStatus.Pending}','{JobStatus.Failed}')");
            where.Append(" AND jobs_tenant.execute_at <= CURRENT_TIMESTAMP()");

            command.CommandText = JobQuery.GetJobs(where.ToString(), orderBy: "jobs_tenant.priority");

            await command.Connection.OpenAsync();

            var dataReader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            var jobs = new List<Job>();

            while (dataReader.Read())
                jobs.Add(JobFactory.Build(dataReader));

            await dataReader.CloseAsync();

            return jobs;
        }

        public async Task<int> JobsUpdateStatusAsync(uint[] ids, JobStatus status, string? message = null)
        {
            using var command = new MySqlConnector.MySqlCommand(JobQuery.UpdateStatus(ids));
            command.Connection = new MySqlConnector.MySqlConnection(ConnectionString);
            command.CommandType = CommandType.Text;

            command.Parameters.AddWithValue("@status", status.ToString());
            command.Parameters.AddWithValue("@message", message ?? DBNull.Value as object);

            await command.Connection.OpenAsync();

            return await command.ExecuteNonQueryAsync();
        }
    }
}

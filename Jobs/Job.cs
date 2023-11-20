using System;

namespace JobsQueue.Jobs
{
    public enum JobKind
    {
        BillAccounting,
        InvoiceAccounting,
    }

    public enum JobPriority
    {
        Lowest,
        BelowNormal,
        Normal,
        AboveNormal,
        Highest,
    }
    
    public enum JobStatus
    {
        Pending,
        Enqueued,
        Running,
        Finished,
        Failed,
    }

    public sealed class Job
    {
        public uint Id { get; set; }
        public uint CompanyKey { get; set; }
        public Guid EntityId { get; set; }
        public JobKind Kind { get; set; }
        public JobPriority Priority { get; set; }
        public JobStatus Status { get; set; }
        public DateTime ExecuteAt { get; set; }
        public string? Message { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }
}

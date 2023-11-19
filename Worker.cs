using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;

namespace JobsQueue
{
    public class Worker : BackgroundService
    {
        private readonly int MAX_THREADS = 60;
        private readonly int MIN_THREADS = 10;
        private static EventWaitHandle eventWaitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        private readonly ConcurrentQueue<Job> _jobsQueueHighestPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueAboveNormalPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueNormalPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueLowestPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueBelowNormalPriority = new ConcurrentQueue<Job>();
        private bool _jobQueuesAreEmpty => (
            _jobsQueueHighestPriority.IsEmpty &&
            _jobsQueueAboveNormalPriority.IsEmpty &&
            _jobsQueueNormalPriority.IsEmpty &&
            _jobsQueueLowestPriority.IsEmpty &&
            _jobsQueueBelowNormalPriority.IsEmpty
        );
        private int _jobsEnqueuedCount => (
            _jobsQueueHighestPriority.Count +
            _jobsQueueAboveNormalPriority.Count +
            _jobsQueueNormalPriority.Count +
            _jobsQueueLowestPriority.Count +
            _jobsQueueBelowNormalPriority.Count
        );
        private Task[] _tasks;
        private List<Job> _totalJobs = new List<Job>();
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _tasks = new Task[MAX_THREADS];
            _logger = logger;
        }

        /// <summary>
        /// Punto de inicio del worker
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Worker started at: {time}", DateTimeOffset.Now);

                var delay = TimeSpan.FromSeconds(0);

                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("\n\nWorker iteration started with {0} jobs enqueued\n\n", _jobsEnqueuedCount);

                    var jobs = await FetchJobs(stoppingToken);

                    if (jobs.Any())
                    {
                        delay = TimeSpan.FromSeconds(10);
                        EnqueueJobs(jobs);
                        // Avisar a los hilos que están esperando para que revisen las colas
                        eventWaitHandle.Set();
                    }

                    SpawnTasks(stoppingToken);

                    _logger.LogInformation("\n\nWorker iteration finished with {0} jobs enqueued\n\n", _jobsEnqueuedCount);

                    await Task.Delay(delay, stoppingToken);
                    delay = delay.Add(TimeSpan.FromSeconds(10));
                }
            }
            catch
            {
                if (_logger.IsEnabled(LogLevel.Information) && stoppingToken.IsCancellationRequested)
                    _logger.LogInformation("Worker has stoped at: {time}", DateTimeOffset.Now);
            }
        }

        protected async Task<List<Job>> FetchJobs(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Fetching jobs...");

            var jobs = new List<Job>();
            var count = RandomNumberGenerator.GetInt32(10) + 1;

            while(--count > 0) {
                jobs.Add(new Job {
                    Id = uint.Parse((_totalJobs.Count + count).ToString()),
                    Kind = (JobKind) RandomNumberGenerator.GetInt32((int) JobKind.AccountingBill, (int) JobKind.AccountingInvoice + 1),
                    Priority = (JobPriority) RandomNumberGenerator.GetInt32((int) JobPriority.Lowest, (int) JobPriority.Highest + 1)
                });
            }

            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

            _logger.LogInformation("Fetched jobs {0}", jobs.Count);

            return jobs;
        }

        protected void EnqueueJobs(List<Job> jobs)
        {
            _logger.LogInformation("Enqueueing {0} jobs...", jobs.Count);
            _totalJobs.AddRange(jobs);

            foreach (var job in jobs)
            {
                switch (job.Priority)
                {
                    case JobPriority.Lowest:
                        _jobsQueueLowestPriority.Enqueue(job);
                        break;
                    case JobPriority.BelowNormal:
                        _jobsQueueBelowNormalPriority.Enqueue(job);
                        break;
                    case JobPriority.Normal:
                        _jobsQueueNormalPriority.Enqueue(job);
                        break;
                    case JobPriority.AboveNormal:
                        _jobsQueueAboveNormalPriority.Enqueue(job);
                        break;
                    case JobPriority.Highest:
                        _jobsQueueHighestPriority.Enqueue(job);
                        break;
                }
            }

            _logger.LogInformation("Enqueueed {0} jobs", jobs.Count);
        }

        protected bool TryDequeueJob([MaybeNullWhen(false)] out Job job)
        {
            if (_jobsQueueHighestPriority.TryDequeue(out job))
                return true;
            if (_jobsQueueAboveNormalPriority.TryDequeue(out job))
                return true;
            if (_jobsQueueNormalPriority.TryDequeue(out job))
                return true;
            if (_jobsQueueBelowNormalPriority.TryDequeue(out job))
                return true;
            if (_jobsQueueLowestPriority.TryDequeue(out job))
                return true;

            return false;
        }

        protected async Task SpawnTask(int index, CancellationToken stoppingToken)
        {
            _logger.LogInformation("Task {index} spawned", index);

            while (!stoppingToken.IsCancellationRequested)
            {
                while ((MIN_THREADS > index || _jobsEnqueuedCount > 0) && TryDequeueJob(out var job))
                    await DoWork(job, index, stoppingToken);

                if (index > MIN_THREADS - 1)
                    break;

                // Esperar a que el hilo principal notifique que hay elementos en la cola
                eventWaitHandle.WaitOne();
            }

            _logger.LogInformation("Task {index} dead", index);
        }

        protected void SpawnTasks(CancellationToken stoppingToken)
        {
            if (_jobsEnqueuedCount == 0)
                return;

            var neededThreads = int.Min(_jobsEnqueuedCount, MAX_THREADS);
            var offset = 0;

            for (int index = 0; index < neededThreads + offset; index++)
            {
                if (_tasks[index] != null && !_tasks[index].IsCompleted)
                {
                    _logger.LogInformation("Thread at {index} is {status}", index, _tasks[index].Status);
                    offset = int.Min(offset + 1, MAX_THREADS);
                    continue;
                }

                if (_tasks[index] != null && _tasks[index].IsCompleted)
                    _tasks[index].Dispose();

                if (_jobsEnqueuedCount == 0)
                    break;

                _tasks[index] = SpawnTask(index, stoppingToken);
            }
        }

        protected async Task DoWork(Job job, int index, CancellationToken stoppingToken)
        {
            var delay = TimeSpan.FromSeconds(RandomNumberGenerator.GetInt32(60 * 2));

            _logger.LogInformation($"[{DateTimeOffset.Now}] thread {index} started Job <Id: {job.Id}, Kind: {job.Kind}, Priority: {job.Priority}>");
            await Task.Delay(delay, stoppingToken);
            _logger.LogInformation($"[{DateTimeOffset.Now}] thread {index} finished Job <Id: {job.Id}, Kind: {job.Kind}, Priority: {job.Priority}>, took {delay.TotalSeconds} seconds");
        }
    }

    public enum JobKind
    {
        AccountingBill,
        AccountingInvoice,
    }
    
    public enum JobPriority
    {
        Lowest,
        BelowNormal,
        Normal,
        AboveNormal,
        Highest,
    }

    public sealed class Job
    {
        public uint Id { get; set; }
        public JobKind Kind { get; set; }
        public JobPriority Priority { get; set; }
    }
}
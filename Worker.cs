using JobsQueue.Jobs;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;

namespace JobsQueue
{
    public class Worker : BackgroundService
    {
        private readonly int MAX_THREADS = 5;
        private readonly int MIN_THREADS = 2;
        private Task[] _tasks;
        private static EventWaitHandle eventWaitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        private readonly ConcurrentQueue<Job> _jobsQueueHighestPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueAboveNormalPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueNormalPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueLowestPriority = new ConcurrentQueue<Job>();
        private readonly ConcurrentQueue<Job> _jobsQueueBelowNormalPriority = new ConcurrentQueue<Job>();
        private int _jobsEnqueuedCount => (
            _jobsQueueHighestPriority.Count +
            _jobsQueueAboveNormalPriority.Count +
            _jobsQueueNormalPriority.Count +
            _jobsQueueLowestPriority.Count +
            _jobsQueueBelowNormalPriority.Count
        );
        private List<Job> _jobs = new List<Job>();
        private readonly JobRepository _jobRepository;
        private readonly IConfiguration _configuration;
        private readonly ILogger<Worker> _logger;

        public Worker(IConfiguration configuration, ILogger<Worker> logger)
        {
            _configuration = configuration;
            _jobRepository = new JobRepository(_configuration);
            _logger = logger;
            _tasks = new Task[MAX_THREADS];
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

                var delay = TimeSpan.FromSeconds(4);

                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("\n\nWorker iteration started with {0} jobs enqueued\n\n", _jobsEnqueuedCount);
                    _jobs = _jobs.AsParallel().Where(job => !new[] { JobStatus.Finished, JobStatus.Failed }.Contains(job.Status)).ToList();

                    var jobs = await _jobRepository.JobsGetAsync();

                    if (jobs.Any())
                    {
                        //delay = TimeSpan.FromSeconds(10);
                        await EnqueueJobs(jobs);
                        _jobs.AddRange(jobs);
                        // Avisar a los hilos que están esperando para que revisen las colas
                        eventWaitHandle.Set();
                    }

                    SpawnTasks(stoppingToken);

                    _logger.LogInformation("\n\nWorker iteration finished with {0} jobs enqueued\n\n", _jobsEnqueuedCount);

                    await Task.Delay(delay, stoppingToken);
                    //delay = delay.Add(TimeSpan.FromSeconds(10));
                }
            }
            catch (Exception ex)
            {
                _jobs = _jobs.AsParallel().Where(job => !new[] { JobStatus.Finished, JobStatus.Failed }.Contains(job.Status)).ToList();

                if (!stoppingToken.IsCancellationRequested)
                {
                    await JobsUpdateAsFailed(ex, _jobs.ToArray());
                    throw;
                }

                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Worker has stoped at: {time}", DateTimeOffset.Now);

                await _jobRepository.JobsUpdateStatusAsync(_jobs.Select(i => i.Id).ToArray(), JobStatus.Pending);
            }
        }

        private async Task EnqueueJobs(List<Job> jobs)
        {
            _logger.LogInformation("Enqueueing {0} jobs...", jobs.Count);

            await _jobRepository.JobsUpdateStatusAsync(jobs.Select(i => i.Id).ToArray(), JobStatus.Enqueued);

            jobs.ForEach(job =>
            {
                job.Status = JobStatus.Enqueued;
            });

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

        private bool TryDequeueJob([MaybeNullWhen(false)] out Job job)
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

        private Task SpawnTask(int index, CancellationToken stoppingToken) => Task.Run(async () =>
        {
            _logger.LogInformation("Task {index} spawned", index);

            while (!stoppingToken.IsCancellationRequested)
            {
                while ((MIN_THREADS > index || _jobsEnqueuedCount > 0) && TryDequeueJob(out var job))
                {
                    try
                    {
                        job.Status = JobStatus.Running;
                        await _jobRepository.JobsUpdateStatusAsync([job.Id], JobStatus.Running);
                        await DoWork(job, index, stoppingToken);
                        await _jobRepository.JobsUpdateStatusAsync([job.Id], JobStatus.Finished);
                        job.Status = JobStatus.Finished;
                    }
                    catch (Exception ex)
                    {
                        await JobsUpdateAsFailed(ex, job);
                    }
                }

                if (index > MIN_THREADS - 1)
                    break;

                // Esperar a que el hilo principal notifique que hay elementos en la cola
                eventWaitHandle.WaitOne();
            }

            _logger.LogInformation("Task {index} dead", index);
        }, stoppingToken);

        private void SpawnTasks(CancellationToken stoppingToken)
        {
            if (_jobsEnqueuedCount == 0)
                return;

            var neededThreads = int.Min(_jobsEnqueuedCount, MAX_THREADS);
            var offset = 0;

            for (int index = 0; index < int.Min(neededThreads + offset, MAX_THREADS - 1); index++)
            {
                _logger.LogInformation("Thread at {index} is {status}", index, _tasks[index]?.Status.ToString() ?? "Empty");

                if (_tasks[index] != null && !_tasks[index].IsCompleted)
                {
                    offset = int.Min(offset + 1, MAX_THREADS);
                    continue;
                }

                if (_tasks[index] != null && _tasks[index].IsCompleted)
                    _tasks[index].Dispose();

                _logger.LogInformation("Thread at {index} new assigned", index);
                _tasks[index] = SpawnTask(index, stoppingToken);
            }
        }

        private async Task JobsUpdateAsFailed(Exception ex, params Job[] jobs)
        {

            var message = new StringBuilder(ex.ToString());

            if (ex.InnerException != null)
                message.AppendLine("|").Append(ex.InnerException);

            message.AppendLine("|").Append(ex.StackTrace);

            foreach (var job in jobs)
            {
                job.Status = JobStatus.Failed;
                job.Message = message.ToString();
            }

            await _jobRepository.JobsUpdateStatusAsync(jobs.Select(i => i.Id).ToArray(), JobStatus.Failed, message.ToString());
        }

        private async Task DoWork(Job job, int index, CancellationToken stoppingToken)
        {
            var delay = TimeSpan.FromSeconds(RandomNumberGenerator.GetInt32(60 * 2));

            _logger.LogInformation($"[{DateTimeOffset.Now}] thread {index} started Job <Id: {job.Id}, Kind: {job.Kind}, Priority: {job.Priority}>");
            await Task.Delay(delay, stoppingToken);
            _logger.LogInformation($"[{DateTimeOffset.Now}] thread {index} finished Job <Id: {job.Id}, Kind: {job.Kind}, Priority: {job.Priority}>, took {delay.TotalSeconds} seconds");
        }
    }
}